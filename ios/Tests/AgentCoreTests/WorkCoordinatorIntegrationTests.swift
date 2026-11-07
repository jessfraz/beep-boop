import Foundation
import Network
import Testing

@testable import AgentCore
@testable import AgentUI

// The runtime test starts the production coordinator with real Pi storage.
// This client never submits model input, starts a worker or retries a job.
@Test(
  .enabled(if: ProcessInfo.processInfo.environment["AGENT_WORK_E2E_SOCKET"] != nil),
  .timeLimit(.minutes(2)))
@MainActor
func nativeWorkInspectionReadsTheRealCoordinatorAndDetachesWithoutStoppingWork() async throws {
  let environment = ProcessInfo.processInfo.environment
  let socket = try #require(environment["AGENT_WORK_E2E_SOCKET"])
  let directory = URL(filePath: try #require(environment["AGENT_WORK_E2E_DIRECTORY"]))
  let jobID = try #require(environment["AGENT_WORK_E2E_JOB"])
  let parentID = try #require(environment["AGENT_WORK_E2E_PARENT"])
  let childID = try #require(environment["AGENT_WORK_E2E_CHILD"])
  let address = try CoordinatorAddress(host: "100.64.1.2")
  let transport: @Sendable (CoordinatorAddress) async throws -> LineConnection = { _ in
    LineConnection(connection: NWConnection(to: .unix(path: socket), using: .tcp))
  }
  let session = CoordinatorSession(
    outboxURL: directory.appending(path: "main-outbox.json"), connectTransport: transport)
  let model = ChatModel()
  func waitFor(_ description: String, _ predicate: () -> Bool) async throws {
    let deadline = ContinuousClock.now + .seconds(20)
    while !predicate() {
      try #require(ContinuousClock.now < deadline, "Native model did not reach \(description)")
      try await Task.sleep(for: .milliseconds(10))
    }
  }
  do {
    await model.connect(session: session, address: address)
    try await waitFor("Main readiness") { model.connection == .connected }
    #expect(model.workInspectionSupported)
    guard case .list(let listing) = try await model.jobs() else {
      Issue.record("Job listing lost its typed diagnostics")
      await model.stop()
      return
    }
    let listed = try #require(listing.jobs.first { $0.job.id == jobID })
    let taskID = try #require(listed.taskId)
    guard case .detail(let detail) = try await model.jobs(jobID) else {
      Issue.record("Job detail lost its typed diagnostics")
      await model.stop()
      return
    }
    #expect(detail.job.taskId == taskID)
    #expect(detail.job.lastRun != nil)
    let page = try await model.conversations(query: taskID)
    let parent = try #require(page.items.first { $0.conversationId == parentID })
    let child = try #require(page.items.first { $0.conversationId == childID })
    #expect(parent.taskId == taskID)
    #expect(parent.agentId == nil)
    #expect(child.taskId == taskID)
    #expect(child.agentId == "review")
    for conversation in [parent, child] {
      let outbox = directory.appending(path: "inspect-\(conversation.conversationId).json")
      let unrelatedOutbox = Data("Preserve the independent Main outbox".utf8)
      try unrelatedOutbox.write(to: outbox)
      let inspector = CoordinatorSession(
        outboxURL: outbox, inspecting: conversation, connectTransport: transport)
      let inspectedModel = ChatModel(inspecting: conversation)
      do {
        await inspectedModel.connect(session: inspector, address: address)
        try await waitFor("saved work readiness") {
          inspectedModel.connection == .connected
        }
        #expect(inspectedModel.inspectedConversation?.conversationId == conversation.conversationId)
        #expect(
          inspectedModel.rows.contains {
            $0.row.text == "Retained \(conversation.conversationId) request"
          })
        #expect(!inspectedModel.workInspectionSupported)
        #expect(!inspectedModel.settingsSupported)
        if conversation.conversationId == parentID {
          let currentRows = Set(inspectedModel.rows.map(\.id))
          await inspectedModel.history(anchorID: nil)
          try await waitFor("conversation-scoped earlier history") {
            !inspectedModel.loadingHistory
              && inspectedModel.rows.contains { !currentRows.contains($0.id) }
          }
          #expect(inspectedModel.error == nil)
          let earlierRows = inspectedModel.rows.filter { !currentRows.contains($0.id) }
          #expect(!earlierRows.isEmpty)
          #expect(earlierRows.allSatisfy { $0.row.text.hasPrefix("Earlier parent evidence ") })
        }
        #expect(await inspector.submit("Inspection must never admit this input") == false)
        await inspectedModel.interrupt()
        await inspectedModel.stop()
        #expect(try Data(contentsOf: outbox) == unrelatedOutbox)
      } catch {
        await inspectedModel.stop()
        throw error
      }
    }
    // Closing both inspection sockets leaves the independent Main observer and
    // the same read-only job/catalog controls usable.
    #expect(try await model.conversations(query: taskID).items.count >= 2)
    await model.stop()
  } catch {
    await model.stop()
    throw error
  }
}

@Test(.timeLimit(.minutes(1)))
func olderCoordinatorsDoNotReceiveUnsupportedWorkControlsOrComposerFallbacks() async throws {
  try await withCoordinator { session, fixture, _ in
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    for await snapshot in session.updates where snapshot.connection == .connected {
      #expect(!snapshot.workInspectionSupported)
      break
    }
    await #expect(throws: (any Error).self) { try await session.jobs() }
    await #expect(throws: (any Error).self) { try await session.conversations() }
    await #expect(throws: (any Error).self) { try await session.retryJob("example-review") }
    #expect(await fixture.count() == 0)
    await session.disconnect()
    do {
      _ = try await session.jobs()
      Issue.record("Disconnected work controls must report their transport state")
    } catch ConnectionError.closed {
      // A disconnected coordinator is distinct from a connected older server.
    } catch { Issue.record(error) }
  }
}
