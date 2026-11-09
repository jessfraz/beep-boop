import Foundation
import Testing

@testable import AgentCore
@testable import AgentUI

private actor ComposerOwnershipGate {
  nonisolated let entered = AsyncStream<Bool>.makeStream()
  private var continuation: CheckedContinuation<Void, Never>?

  func wait(after admitted: Bool) async -> Bool {
    await withCheckedContinuation { continuation in
      self.continuation = continuation
      entered.continuation.yield(admitted)
    }
    return admitted
  }
  func finish() {
    continuation?.resume()
    continuation = nil
  }
}

enum ComposerOwnershipStage: CaseIterable, Sendable {
  case admitted, accepted, placed, retired, liveRetry, relocated
}

@MainActor
@Test(.timeLimit(.minutes(1)), arguments: ComposerOwnershipStage.allCases, [false, true])
func admittedComposerOwnershipSurvivesFailedCheckpointAndColdRestore(
  stage: ComposerOwnershipStage, newTyping: Bool
) async throws {
  try await withCoordinator { session, fixture, outbox in
    if stage == .admitted || stage == .liveRetry || stage == .relocated {
      await fixture.loseInputAcknowledgements()
    }
    if stage == .retired { await fixture.refuseUploads() }
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    for await update in session.updates where update.connection == .connected { break }
    let root = outbox.deletingLastPathComponent()
    let directory = root.appending(path: "Composer")
    let url = directory.appending(path: "draft.json")
    let files = AttachmentStore(directory: root.appending(path: "Drafts"))
    let gate = ComposerOwnershipGate()
    let model = await MainActor.run {
      ChatModel(
        draftStore: files, persistence: ComposerDraftStore(url: url), outboxURL: outbox,
        admit: { text, mode, files, composer in
          let admitted = await session.submit(text, mode: mode, files: files, composer: composer)
          return await gate.wait(after: admitted)
        })
    }
    await model.restoreDraft()
    let original = "Two different drafts can contain this exact same text."
    await MainActor.run {
      model.connection = .connected
      model.draft = original
    }
    await model.attach(
      data: Data("Original file".utf8), name: "Original.txt", mediaType: "text/plain")
    try #require(await model.saveDraft())
    let originalFile = try #require(await model.attachments.first)
    let sending = Task { await model.send() }
    for await admitted in gate.entered.stream {
      try #require(admitted)
      break
    }
    let input = try #require(savedInputs(at: outbox).first)
    #expect(input.text == original)
    switch stage {
    case .admitted, .liveRetry, .relocated: break
    case .accepted:
      for await update in session.updates
      where update.rows.contains(where: { $0.inputID == input.inputId && $0.delivery == .saved }) {
        break
      }
    case .placed:
      let deadline = ContinuousClock.now + .seconds(8)
      while await fixture.count() == 0 {
        try #require(ContinuousClock.now < deadline)
        try await Task.sleep(for: .milliseconds(5))
      }
      await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
      for await update in session.updates
      where update.connection == .connected
        && update.rows.contains(where: { $0.entryID == "placed-\(input.inputId)" })
      {
        break
      }
      #expect(try savedInputs(at: outbox).isEmpty)
    case .retired:
      for await update in session.updates
      where update.rows.contains(where: { $0.inputID == input.inputId && $0.delivery == .rejected })
      {
        break
      }
      let recoveredFiles = AttachmentStore(directory: root.appending(path: "RecoveredFiles"))
      let recovered = try #require(await session.restore(input.inputId, to: recoveredFiles))
      try await ComposerDraftStore(url: root.appending(path: "recovered.json")).save(
        ComposerDraft(
          text: recovered.text, mode: recovered.mode, attachments: recovered.attachments),
        revision: 1)
      #expect(await session.commitRestoration(recovered) == .committed)
      #expect(try savedInputs(at: outbox).isEmpty)
    }
    let newText = newTyping ? original : ""
    if newTyping {
      await MainActor.run {
        model.draft = newText
        model.mode = .followUp
      }
      await model.attach(data: Data("New file".utf8), name: "New.txt", mediaType: "text/plain")
      try #require(await model.saveDraft())
    }
    try FileManager.default.setAttributes([.posixPermissions: 0o500], ofItemAtPath: directory.path)
    defer {
      try? FileManager.default.setAttributes(
        [.posixPermissions: 0o700], ofItemAtPath: directory.path)
    }
    await gate.finish()
    await sending.value
    #expect(await model.draft == newText)
    #expect(await model.attachments.map(\.reference.name) == (newTyping ? ["New.txt"] : []))
    #expect(await model.draftError != nil)

    var coldRoot = root
    var coldDirectory = directory
    var coldFiles = files
    var originalFileURL = originalFile.url
    if stage == .relocated {
      await session.disconnect()
      coldRoot = root.deletingLastPathComponent().appending(path: UUID().uuidString)
      try FileManager.default.moveItem(at: root, to: coldRoot)
      coldDirectory = coldRoot.appending(path: "Composer")
      coldFiles = AttachmentStore(directory: coldRoot.appending(path: "Drafts"))
      originalFileURL = try #require(await coldFiles.existing(originalFile.reference)).url
    }
    defer {
      if stage == .relocated {
        try? FileManager.default.setAttributes(
          [.posixPermissions: 0o700], ofItemAtPath: coldDirectory.path)
        try? FileManager.default.removeItem(at: coldRoot)
      }
    }

    // A different endpoint must not hide the admission proof in the original
    // endpoint's outbox. Receipt retirement must not hide it either.
    let cold: ChatModel
    if stage == .liveRetry {
      cold = model
    } else {
      cold = await MainActor.run {
        ChatModel(
          draftStore: coldFiles,
          persistence: ComposerDraftStore(url: coldDirectory.appending(path: "draft.json")),
          outboxURL: coldRoot.appending(path: "OtherEndpoint/outbox.json"))
      }
      await cold.restoreDraft()
    }
    #expect(FileManager.default.fileExists(atPath: originalFileURL.path))
    #expect(await cold.draft == newText)
    #expect(await cold.attachments.map(\.reference.name) == (newTyping ? ["New.txt"] : []))
    if newTyping {
      #expect(await cold.mode == .followUp)
      let file = try #require(await cold.attachments.first)
      #expect(try Data(contentsOf: file.url) == Data("New file".utf8))
    }
    try FileManager.default.setAttributes(
      [.posixPermissions: 0o700], ofItemAtPath: coldDirectory.path)
    try #require(await cold.saveDraft())
    #expect(!FileManager.default.fileExists(atPath: originalFileURL.path))
    if newTyping {
      let file = try #require(await cold.attachments.first)
      #expect(try Data(contentsOf: file.url) == Data("New file".utf8))
    }
  }
}

@MainActor
@Test(
  .timeLimit(.minutes(1)), arguments: [(false, false), (true, false), (false, true), (true, true)])
func failedInitialComposerOrOutboxPersistenceDoesNotClaimTheDraft(
  outboxFailure: Bool, control: Bool
) async throws {
  try await withCoordinator { session, fixture, outbox in
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    for await update in session.updates where update.connection == .connected { break }
    let directory = outbox.deletingLastPathComponent().appending(path: "Composer")
    let url = directory.appending(path: "draft.json")
    let files = AttachmentStore(directory: directory.appending(path: "Files"))
    let model = await MainActor.run {
      ChatModel(
        draftStore: files, persistence: ComposerDraftStore(url: url), outboxURL: outbox,
        admit: { text, mode, files, composer in
          await session.submit(text, mode: mode, files: files, composer: composer)
        })
    }
    await model.restoreDraft()
    let text = control ? "/status" : "Keep this unadmitted note."
    await MainActor.run {
      model.connection = .connected
      model.draft = text
    }
    if !control {
      await model.attach(data: Data("Keep file".utf8), name: "Keep.txt", mediaType: "text/plain")
    }
    try #require(await model.saveDraft())
    if outboxFailure {
      try FileManager.default.createDirectory(at: outbox, withIntermediateDirectories: true)
    } else {
      try FileManager.default.setAttributes(
        [.posixPermissions: 0o500], ofItemAtPath: directory.path)
    }
    defer {
      try? FileManager.default.setAttributes(
        [.posixPermissions: 0o700], ofItemAtPath: directory.path)
    }
    await model.send()
    #expect(await model.draft == text)
    #expect(await fixture.count() == 0)
    if outboxFailure {
      try FileManager.default.removeItem(at: outbox)
      // Rejection restores the live draft in defer and schedules its autosave.
      // Finish that owner before simulating a cold process with another store.
      try #require(await model.saveDraft())
    }
    let cold = await MainActor.run {
      ChatModel(draftStore: files, persistence: ComposerDraftStore(url: url))
    }
    await cold.restoreDraft()
    #expect(await cold.draft == text)
    #expect(await cold.attachments.map(\.reference.name) == (control ? [] : ["Keep.txt"]))
  }
}

@MainActor @Test(.timeLimit(.minutes(1)), arguments: [false, true])
func acceptedSlashControlDoesNotReturnFromAFailedComposerCheckpoint(newTyping: Bool) async throws {
  try await withCoordinator { session, fixture, outbox in
    let address = try CoordinatorAddress(host: "100.64.1.2")
    await session.connect(address)
    for await update in session.updates where update.connection == .connected { break }
    let directory = outbox.deletingLastPathComponent().appending(path: "Composer")
    let url = directory.appending(path: "draft.json")
    let files = AttachmentStore(directory: directory.appending(path: "Files"))
    let gate = ComposerOwnershipGate()
    let model = await MainActor.run {
      ChatModel(
        draftStore: files, persistence: ComposerDraftStore(url: url), outboxURL: outbox,
        admit: { text, mode, files, composer in
          let admitted = await session.submit(text, mode: mode, files: files, composer: composer)
          return await gate.wait(after: admitted)
        })
    }
    await model.restoreDraft()
    await MainActor.run {
      model.connection = .connected
      model.draft = "/status"
    }
    let sending = Task { await model.send() }
    for await admitted in gate.entered.stream {
      try #require(admitted)
      break
    }
    for await input in fixture.observedInputs {
      #expect(input.text == "/status")
      #expect(input.attachments == nil)
      break
    }
    let newText = newTyping ? "/status" : ""
    if newTyping {
      await MainActor.run { model.draft = newText }
      await model.attach(data: Data("New note".utf8), name: "New.txt", mediaType: "text/plain")
      try #require(await model.saveDraft())
    }
    try FileManager.default.setAttributes([.posixPermissions: 0o500], ofItemAtPath: directory.path)
    defer {
      try? FileManager.default.setAttributes(
        [.posixPermissions: 0o700], ofItemAtPath: directory.path)
    }
    await gate.finish()
    await sending.value
    #expect(await model.draft == newText)
    #expect(await model.draftError != nil)
    let cold = await MainActor.run {
      ChatModel(draftStore: files, persistence: ComposerDraftStore(url: url))
    }
    await cold.restoreDraft()
    #expect(await cold.draft == newText)
    #expect(await cold.attachments.map(\.reference.name) == (newTyping ? ["New.txt"] : []))
    if newTyping {
      let file = try #require(await cold.attachments.first)
      #expect(try Data(contentsOf: file.url) == Data("New note".utf8))
    }
    if FileManager.default.fileExists(atPath: outbox.path) {
      #expect(try savedInputs(at: outbox).isEmpty, "Controls must not enter the replayable queue")
    }
    await session.connect(address)
    for await update in session.updates where update.connection == .connected {
      #expect(update.rows.isEmpty)
      break
    }
    #expect(await fixture.count() == 1, "Reconnection must not submit the control again")
  }
}
