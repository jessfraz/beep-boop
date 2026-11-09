import Foundation
import Testing

@testable import AgentCore
@testable import AgentUI

private func inspectionJob(
  id: String = "example-review", state: JobExecutionState? = .completed,
  output: String? = "The retained review completed.\nNo repeated actions were needed.",
  scheduleError: String? = nil
) -> ScheduledJobStatus {
  let scheduled = 1_798_797_600_000
  let taskID = "11111111-1111-4111-8111-111111111111"
  return ScheduledJobStatus(
    job: .init(
      cwd: "/workspace/example", id: id,
      model: .init(fast: false, modelId: "example-model", provider: .openai, reasoning: .high),
      prompt: "Review the existing evidence and keep unfinished work with its original owner.",
      revision: "current-revision", schedules: [], timezone: "America/Los_Angeles",
      title: id == "example-review" ? "Example review" : "Another review"),
    lastRun: state.map {
      .init(
        finishedAt: $0 == .completed ? scheduled + 60_000 : nil, output: output,
        outputTruncated: false, revision: "captured-revision", scheduleId: "daily",
        scheduledAt: scheduled, startedAt: scheduled + 10, state: $0, taskId: taskID)
    },
    schedules: [
      .init(
        error: scheduleError, lastAt: scheduled, nextAt: scheduled + 86_400_000, scheduleId: "daily"
      )
    ],
    taskId: taskID)
}

@Test func workInspectionKeepsFullDiagnosticsAndRecoveryDistinctFromNormalWork() {
  let message = "Retained native task needs inspection.\nThe original input remains saved. 🦀"
  let parked = inspectionJob(state: .blocked(.init(message: "restart_limit")))
  #expect(parked.canRetryInterruptedRun)
  #expect(parked.lastRun?.state.workLabel == "Parked")
  let failed = inspectionJob(
    state: .failed(.init(message: message)), output: "first\nsecond\nthird")
  #expect(failed.lastRun?.state.workDiagnostic == message)
  #expect(failed.lastRun?.output == "first\nsecond\nthird")
  #expect(!failed.canRetryInterruptedRun)
  let diagnostic =
    "Background work is parked after repeated interrupted restarts; inspect its native task before retrying"
  #expect(inspectionJob(state: nil, scheduleError: diagnostic).canRetryInterruptedRun)
  #expect(!inspectionJob(state: .running, scheduleError: diagnostic).canRetryInterruptedRun)
  #expect(!inspectionJob(state: .waiting, scheduleError: diagnostic).canRetryInterruptedRun)
  #expect(
    !inspectionJob(state: .retrying(.init(at: 1)), scheduleError: diagnostic).canRetryInterruptedRun
  )
  #expect(
    !inspectionJob(state: .blocked(.init(message: "missing_task")), scheduleError: diagnostic)
      .canRetryInterruptedRun)
  let legacy = inspectionJob(state: nil)
  #expect(legacy.lastRun == nil)
  #expect(legacy.taskId != nil)
  var recoveredOwner = inspectionJob()
  let capturedOwner = recoveredOwner.lastRun?.taskId
  recoveredOwner.taskId = "22222222-2222-4222-8222-222222222222"
  #expect(recoveredOwner.inspectionTaskID == capturedOwner)
  recoveredOwner.lastRun = nil
  #expect(recoveredOwner.inspectionTaskID == recoveredOwner.taskId)
  #expect(inspectionJob(output: "").lastRun?.output == "")
  #expect(inspectionJob(output: nil).lastRun?.output == nil)
}

@MainActor @Test func workRelationshipsUseOwningIdentityInsteadOfSimilarTitles() {
  let parent = SavedConversation(conversationId: "10", taskId: "owner", title: "Review")
  let child = SavedConversation(
    agentId: "first", conversationId: "11", taskId: "owner", title: "Review")
  let sibling = SavedConversation(
    agentId: "second", conversationId: "12", taskId: "owner", title: "Another review")
  let unrelated = SavedConversation(
    agentId: "first", conversationId: "13", taskId: "different-owner", title: "Review")
  let imported = SavedConversation(
    conversationId: "14", sourceThreadId: "saved-thread", title: "Review")
  let items = [unrelated, child, sibling, imported, parent]
  #expect(
    ConversationInspectionView.relationships(for: child, in: items).map(\.conversationId) == [
      "10", "12",
    ])
  #expect(ConversationInspectionView.relationships(for: imported, in: items).isEmpty)
  #expect(
    ConversationInspectionView.relationships(for: parent, in: items).map(\.conversationId) == [
      "12", "11",
    ])
}

#if os(iOS) && AGENT_APP_HOST
  import CoreGraphics
  import SwiftUI
  import UIKit

  // Keep hosted captures in the existing serialized suite that owns the key
  // window. They render product views with invented typed data, not a scheduler.
  extension NativeRenderingTests {
    @MainActor private static func captureWork<Content: View>(
      _ content: Content, name: String, size: DynamicTypeSize = .large,
      scheme: ColorScheme = .light, dimensions: CGSize = CGSize(width: 390, height: 844),
      exercise: (@MainActor (UIViewController, UIWindow) async throws -> Void)? = nil,
      includePresentedView: Bool = false
    ) async throws {
      let host = UIHostingController(
        rootView: content.environment(\.dynamicTypeSize, size).environment(\.colorScheme, scheme))
      let scene = try #require(
        UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
      let previousWindow = scene.windows.first(where: \.isKeyWindow)
      let window = UIWindow(windowScene: scene)
      window.overrideUserInterfaceStyle = scheme == .dark ? .dark : .light
      let container = UIViewController()
      window.rootViewController = container
      window.makeKeyAndVisible()
      defer {
        window.isHidden = true
        host.willMove(toParent: nil)
        host.view.removeFromSuperview()
        host.removeFromParent()
        window.rootViewController = nil
        previousWindow?.makeKey()
      }
      window.layoutIfNeeded()
      container.addChild(host)
      container.view.addSubview(host.view)
      host.view.frame = CGRect(origin: .zero, size: dimensions)
      host.didMove(toParent: container)
      try await Task.sleep(for: .milliseconds(150))
      host.view.layoutIfNeeded()
      try await exercise?(host, window)
      try await Task.sleep(for: .milliseconds(100))
      let format = UIGraphicsImageRendererFormat()
      format.scale = 1
      format.preferredRange = .standard
      let hostedView = try #require(host.view)
      let surface: UIView = includePresentedView ? window : hostedView
      let imageSize = includePresentedView ? window.bounds.size : dimensions
      let image = UIGraphicsImageRenderer(size: imageSize, format: format).image { _ in
        #expect(surface.drawHierarchy(in: surface.bounds, afterScreenUpdates: true))
      }
      let pixels = try #require(image.cgImage)
      #expect(pixels.width == Int(imageSize.width))
      #expect(pixels.height == Int(imageSize.height))
      Attachment.record(pixels, named: name, as: .png)
    }

    @MainActor @Test func nativeAttachmentQuickLookUsesTheExistingProductPreview() async throws {
      let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
      try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
      defer { try? FileManager.default.removeItem(at: directory) }
      let url = directory.appending(path: "Example notes.pdf")
      let renderer = UIGraphicsPDFRenderer(bounds: CGRect(x: 0, y: 0, width: 400, height: 520))
      try renderer.writePDF(to: url) { context in
        context.beginPage()
        ("Example retained attachment\n\nThe preview opens without sending another input."
          as NSString)
          .draw(
            in: CGRect(x: 30, y: 30, width: 340, height: 300),
            withAttributes: [.font: UIFont.systemFont(ofSize: 20)])
      }
      let model = ChatModel()
      model.previewURL = url
      try await Self.captureWork(
        MainChatView(model: model), name: "native-attachment-quick-look",
        exercise: { host, window in
          let deadline = ContinuousClock.now + .seconds(3)
          while host.presentedViewController == nil
            && window.rootViewController?.presentedViewController == nil
            && ContinuousClock.now < deadline
          {
            try await Task.sleep(for: .milliseconds(20))
          }
          #expect(
            host.presentedViewController != nil
              || window.rootViewController?.presentedViewController != nil)
        }, includePresentedView: true)
    }

    @MainActor @Test func nativeJobsAndWorkScreensRetainStatesUnknownTimingAndRelationships()
      async throws
    {
      let parent = SavedConversation(
        conversationId: "10", host: "Example worker", taskId: "owner", title: "Example review",
        updatedAt: 1_798_797_600_000)
      let child = SavedConversation(
        agentId: "evidence", conversationId: "11", host: "Example worker", taskId: "owner",
        title: "Read existing evidence", updatedAt: 1_798_797_600_010)
      let parked = inspectionJob(
        state: .blocked(.init(message: "restart_limit")), output: nil,
        scheduleError:
          "Background work is parked after repeated interrupted restarts; inspect its native task before retrying"
      )
      let running = inspectionJob(id: "another-review", state: .running, output: nil)
      for scheme in [ColorScheme.light, .dark] {
        try await Self.captureWork(
          NavigationStack {
            List {
              WorkCatalogSections(
                model: ChatModel(), jobs: [parked, running], conversations: [parent, child],
                inspect: { _ in })
            }
            .navigationTitle("Jobs and work")
          }, name: "jobs-work-catalog-\(scheme)", scheme: scheme)
      }
      try await Self.captureWork(
        NavigationStack {
          Form { JobDiagnosticSections(status: parked) }.navigationTitle("Example review")
        },
        name: "job-parked-error-detail")
      try await Self.captureWork(
        NavigationStack {
          Form { JobDiagnosticSections(status: inspectionJob(state: nil)) }.navigationTitle(
            "Older run")
        },
        name: "job-legacy-timing-unknown")
      try await Self.captureWork(
        NavigationStack {
          Form { JobDiagnosticSections(status: inspectionJob()) }.navigationTitle("Completed run")
        },
        name: "job-completed-output")
      let model = ChatModel(inspecting: child)
      model.connection = .connected
      model.model = "example/model"
      var transcript = Transcript()
      transcript.message(
        .init(
          continuation: false, entryId: "1", finalChunk: true, role: .user,
          text: "Inspect the existing source evidence."))
      transcript.message(
        .init(
          continuation: false, entryId: "2", finalChunk: true, role: .assistant,
          text:
            "The original task and saved input remain intact. This view observes their current progress."
        ))
      model.rows = transcript.rows.map(RowModel.init)
      try await Self.captureWork(
        MainChatView(model: model, relatedWork: [parent], inspectRelated: { _ in }),
        name: "readonly-child-conversation")
    }

    @MainActor @Test func nativeSourceErrorAndToolInputDetailsRemainInspectable() async throws {
      let model = ChatModel()
      model.connection = .connected
      model.sources = [
        .init(
          checkedAt: 1_798_797_600_000,
          coverage: .unavailable(
            .init(
              message:
                "The local source reader could not open its database. Check the configured worker’s access before retrying."
            )), source: .init(account: "local", provider: "messages")),
        .init(
          checkedAt: 1_798_797_600_010, coverage: .complete,
          source: .init(account: "example-account", provider: "google")),
      ]
      try await Self.captureWork(
        NavigationStack { SourceDetailsView(model: model) }, name: "sources-actionable-error")
      struct Input: Encodable {
        let program = "git"
        let args = ["status", "--short"]
        let cwd = "/workspace/example"
      }
      let encoder = JSONEncoder()
      encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
      let input = String(decoding: try encoder.encode(Input()), as: UTF8.self)
      try await Self.captureWork(
        NavigationStack {
          ScrollView {
            VStack(alignment: .leading, spacing: 16) {
              Text("Finished").font(.caption)
              ToolContent(title: "Input", content: .text(.init(text: input, truncated: false)))
              ToolContent(
                title: "Output", content: .text(.init(text: "No changed files.", truncated: false)))
            }.padding()
          }.navigationTitle("Tool details")
        }, name: "tool-full-input-detail")
    }

    @MainActor @Test func nativeConnectedConversationSettingsUsesExistingReadControl() async throws
    {
      try await withCoordinator { session, fixture, _ in
        await fixture.advertiseSettings(true)
        await fixture.replyToSettings(
          .init(
            configured: .init(
              catalog: [
                .init(
                  fast: true, modelId: "example-model", name: "Example model", provider: .openai,
                  reasoning: [.low, .high, .xhigh])
              ],
              conversationId: "fixture-root",
              model: .init(
                fast: false, modelId: "example-model", provider: .openai, reasoning: .high))))
        let model = await MainActor.run { ChatModel() }
        await model.connect(session: session, address: try CoordinatorAddress(host: "100.64.1.2"))
        // Readiness uses the session's handshake budget, not a rendering deadline.
        let deadline = ContinuousClock.now + .seconds(30)
        while await model.connection != .connected, ContinuousClock.now < deadline {
          try await Task.sleep(for: .milliseconds(20))
        }
        try #require(await model.connection == .connected)
        try #require(await model.settingsSupported)
        await model.refreshSettings()
        #expect(await model.conversationSettings != nil)
        try await Self.captureWork(
          NavigationStack { ConversationSettingsView(model: model) },
          name: "connected-conversation-settings")
        #expect(await fixture.count() == 0)
        await model.stop()
      }
    }
  }
#endif
