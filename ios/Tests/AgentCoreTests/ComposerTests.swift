import AgentCore
import Foundation
import Testing

@testable import AgentUI

private actor AdmissionGate {
  nonisolated let entered: AsyncStream<Void>
  private let enteredContinuation: AsyncStream<Void>.Continuation
  private var continuation: CheckedContinuation<Bool, Never>?
  private var messages: [String] = []
  init() {
    let pair = AsyncStream<Void>.makeStream()
    entered = pair.stream
    enteredContinuation = pair.continuation
  }
  func admit(_ text: String) async -> Bool {
    messages.append(text)
    return await withCheckedContinuation { continuation in
      self.continuation = continuation
      enteredContinuation.yield(())
    }
  }
  func count() -> Int { messages.count }
  func finish(_ admitted: Bool) {
    continuation?.resume(returning: admitted)
    continuation = nil
  }
}

@MainActor @Test(.timeLimit(.minutes(1)))
func buttonSubmissionSnapshotsAndClearsBeforeReturning() async throws {
  try await withCoordinator { session, fixture, outbox in
    await fixture.loseInputAcknowledgements()
    let model = await MainActor.run { ChatModel() }
    await model.connect(session: session, address: try CoordinatorAddress(host: "100.64.1.2"))
    try await waitForComposerState { model.connection == .connected }
    let text = "Copy this note before clearing the visible editor."
    let started = await MainActor.run {
      model.draft = text
      let started = model.startSend()
      #expect(model.draft.isEmpty)
      #expect(model.admitting)
      model.draft = text
      return started
    }
    let sending = try #require(started)
    await sending.value
    #expect(await model.draft == text, "New identical typing belongs to the next message")
    #expect(try savedInputs(at: outbox).map(\.text) == [text])
    await model.stop()
  }
}

@MainActor
@Test(arguments: [
  ("", "/feedback "),
  ("Keep the note.\nSecond line.", "/feedback Keep the note.\nSecond line."),
  ("/help Keep the note.", "/feedback Keep the note."),
  ("/feedback Keep the note.", "/feedback Keep the note."),
  (" \n/help\tKeep the note.", "/feedback\tKeep the note."),
  ("/help", "/feedback "),
  ("/helpful is a path", "/feedback /helpful is a path"),
  ("/tmp/example.txt", "/feedback /tmp/example.txt"),
])
func commandInsertionPreservesTheDurableNoteAndReplacesOnlyKnownCommands(
  draft: String, expected: String
) async throws {
  let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: root) }
  let files = AttachmentStore(directory: root.appending(path: "Files"))
  let url = root.appending(path: "draft.json")
  let model = ChatModel(draftStore: files, persistence: ComposerDraftStore(url: url))
  await model.restoreDraft()
  let feedback = SlashCommand(description: "Report a problem", usage: "/feedback <note>")
  model.commands = [feedback, .init(description: "Show commands", usage: "/help")]
  model.mode = .followUp
  await model.attach(data: Data("Kept attachment".utf8), name: "Note.txt", mediaType: "text/plain")
  let attachment = try #require(model.attachments.first)
  model.draft = draft
  model.insertCommand(feedback)
  #expect(model.draft == expected)
  let saved = await model.saveDraft()
  try #require(saved, "Draft save failed: \(model.draftError ?? "unknown error")")
  let restored = try await ComposerDraftStore(url: url).load(from: files)
  #expect(restored.draft.text == expected)
  #expect(restored.draft.mode == .followUp)
  #expect(restored.draft.attachments == [attachment])
}

@MainActor private func waitForComposerState(_ predicate: @MainActor () -> Bool) async throws {
  let deadline = ContinuousClock.now + .seconds(8)
  while !predicate() {
    guard ContinuousClock.now < deadline else { throw ConnectionError.timedOut }
    try await Task.sleep(for: .milliseconds(5))
  }
}

@MainActor @Test(.timeLimit(.minutes(1)))
func failedDraftRestorationKeepsTheOriginalDurableOutbox() async throws {
  try await withCoordinator { session, fixture, outbox in
    let directory = outbox.deletingLastPathComponent()
    let files = AttachmentStore(directory: directory.appending(path: "Drafts"))
    let draftURL = directory.appending(path: "composer.json")
    let model = await MainActor.run {
      ChatModel(draftStore: files, persistence: ComposerDraftStore(url: draftURL))
    }
    await model.restoreDraft()
    await fixture.refuseUploads()
    await model.connect(session: session, address: try CoordinatorAddress(host: "100.64.1.2"))
    try await waitForComposerState { model.connection == .connected }
    let file = try await files.stage(
      data: Data("Keep the original file".utf8), name: "Note.txt", mediaType: "text/plain")
    #expect(await session.submit("Restore the original message", files: [file]))
    try await waitForComposerState { model.rows.last?.row.delivery == .rejected }
    let inputID = try #require(await MainActor.run { model.rows.last?.row.inputID })
    let original = try Data(contentsOf: outbox)

    // A real second owner makes the replacement save fail without a fake store.
    let otherOwner = ComposerDraftStore(url: draftURL)
    let adopted = try await otherOwner.load(from: files)
    try await otherOwner.save(
      ComposerDraft(text: "Another unsent note"), revision: adopted.revision + 1)
    await model.restore(inputID)
    #expect(try Data(contentsOf: outbox) == original)
    #expect(await model.draftError != nil)
    let reopened = try await ComposerDraftStore(url: draftURL).load(from: files)
    #expect(reopened.draft.text == "Another unsent note")
    await model.stop()
  }
}

@MainActor @Test(.timeLimit(.minutes(1)))
func sendClearsComposerBeforeAdmissionAndPreservesNewTyping() async {
  let gate = AdmissionGate()
  let model = ChatModel(admit: { text, _, _, _ in await gate.admit(text) })
  model.connection = .connected
  model.draft = "Original draft"
  let sending = Task { await model.send() }
  for await _ in gate.entered { break }
  #expect(model.draft.isEmpty, "Send must immediately clear the composer during admission")
  model.draft = "New typing while admission is pending"
  await model.send()
  #expect(await gate.count() == 1)
  await gate.finish(true)
  await sending.value
  #expect(model.draft == "New typing while admission is pending")
  #expect(!model.admitting)
}

@MainActor @Test(.timeLimit(.minutes(1)), arguments: [false, true])
func durableAdmissionDoesNotEraseANewerDraftWithIdenticalText(afterClearing: Bool) async throws {
  try await withCoordinator { session, _, outbox in
    let gate = AdmissionGate()
    let model = await MainActor.run {
      ChatModel(admit: { text, mode, files, _ in
        let admitted = await session.submit(text, mode: mode, files: files)
        #expect(admitted)
        return await gate.admit(text)
      })
    }
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    for await update in session.updates where update.connection == .connected { break }
    await MainActor.run {
      model.connection = .connected
      model.draft = "The same text in a different draft"
    }
    let sending = Task { await model.send() }
    for await _ in gate.entered { break }
    let pending = try savedInputs(at: outbox)
    #expect(pending.count == 1)
    await MainActor.run {
      if afterClearing { model.draft = "" }
      model.draft = "The same text in a different draft"
    }
    await model.send()
    #expect(await gate.count() == 1)
    await gate.finish(true)
    await sending.value
    #expect(await model.draft == "The same text in a different draft")
    #expect(await !model.admitting)
  }
}

@MainActor @Test(.timeLimit(.minutes(1)), arguments: [false, true])
func savedAdmissionStaysRetiredWhenTheConnectionOwnerStops(newerDraft: Bool) async throws {
  try await withCoordinator { session, _, outbox in
    let gate = AdmissionGate()
    let directory = outbox.deletingLastPathComponent()
    let draftURL = directory.appending(path: "composer.json")
    let files = AttachmentStore(directory: directory.appending(path: "Drafts"))
    let model = await MainActor.run {
      ChatModel(
        draftStore: files, persistence: ComposerDraftStore(url: draftURL),
        admit: { text, mode, files, _ in
          let saved = await session.submit(text, mode: mode, files: files)
          #expect(saved)
          return await gate.admit(text)
        })
    }
    await model.restoreDraft()
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    for await update in session.updates where update.connection == .connected { break }
    await MainActor.run {
      model.connection = .connected
      model.draft = "The same text in a different draft"
    }
    #expect(await model.saveDraft())
    let sending = Task { await model.send() }
    for await _ in gate.entered { break }
    let saved = try savedInputs(at: outbox)
    #expect(saved.count == 1)
    if newerDraft {
      await MainActor.run { model.draft = "The same text in a different draft" }
    }
    await model.stop()
    await session.disconnect()
    await gate.finish(true)
    await sending.value
    let expected = newerDraft ? "The same text in a different draft" : ""
    #expect(await model.draft == expected)
    let restored = try await ComposerDraftStore(url: draftURL).load(from: files)
    #expect(restored.draft.text == expected)
    #expect(await !model.admitting)
  }
}

@MainActor @Test(.timeLimit(.minutes(1)))
func sendKeepsAnUnadmittedDraftAndClearsOnlyItsAdmittedOriginal() async {
  let model = ChatModel(admit: { _, _, _, _ in false })
  model.connection = .connected
  model.draft = "Not saved"
  await model.send()
  #expect(model.draft == "Not saved")
  let admitted = ChatModel(admit: { _, _, _, _ in true })
  admitted.connection = .connected
  admitted.draft = "Saved original"
  await admitted.send()
  #expect(admitted.draft.isEmpty)
}

@MainActor @Test(.timeLimit(.minutes(1)), arguments: [false, true], ["", "New typing"])
func sendingKeepsItsPersistedBackupUntilAdmissionFinishes(admitted: Bool, newText: String)
  async throws
{
  let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: root) }
  let files = AttachmentStore(directory: root.appending(path: "Files"))
  let url = root.appending(path: "draft.json")
  let gate = AdmissionGate()
  let model = ChatModel(
    draftStore: files, persistence: ComposerDraftStore(url: url),
    admit: { text, _, _, _ in await gate.admit(text) })
  await model.restoreDraft()
  model.connection = .connected
  model.draft = "Original draft"
  #expect(await model.saveDraft())
  let sending = Task { await model.send() }
  for await _ in gate.entered { break }
  #expect(model.draft.isEmpty)
  if !newText.isEmpty {
    model.draft = newText
    #expect(await model.saveDraft())
  }
  let backup = "Original draft" + (newText.isEmpty ? "" : "\n\n" + newText)
  let saved = try await ComposerDraftStore(url: url).load(from: files)
  #expect(saved.draft.text == backup)
  await gate.finish(admitted)
  await sending.value
  #expect(model.draft == (admitted ? newText : backup))
  // Rejection schedules an autosave. Finish it before another owner reloads.
  #expect(await model.saveDraft())
  let restored = try await ComposerDraftStore(url: url).load(from: files)
  #expect(restored.draft.text == model.draft)
}

@MainActor @Test(.timeLimit(.minutes(1)), arguments: [false, true])
func rejectedAdmissionRestoresItsTextWithoutLosingNewTyping(stopping: Bool) async {
  let gate = AdmissionGate()
  let model = ChatModel(admit: { text, _, _, _ in await gate.admit(text) })
  model.connection = .connected
  model.draft = "Original draft"
  let sending = Task { await model.send() }
  for await _ in gate.entered { break }
  model.draft = "New typing"
  if stopping { await model.stop() }
  await gate.finish(false)
  await sending.value
  #expect(model.draft == "Original draft\n\nNew typing")
}

@MainActor @Test(.timeLimit(.minutes(1)))
func feedbackCapturesOnceAndFailedAdmissionKeepsTheExactEvidence() async throws {
  let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: root) }
  let model = ChatModel(
    draftStore: AttachmentStore(directory: root), admit: { _, _, _, _ in false })
  model.connection = .connected
  model.draft = "/feedback The text is clipped"
  var captures = 0
  await model.send(screenshot: {
    captures += 1
    return Data([1, 2, 3])
  })
  let original = model.attachments
  #expect(original.map(\.reference.name) == ["Feedback.png", "Composer.log"])
  let image = try #require(original.first)
  let diagnostics = try #require(original.last)
  let log = try String(contentsOf: diagnostics.url, encoding: .utf8)
  #expect(log.hasPrefix("Composer diagnostics: coverage="))
  #expect(!log.contains("The text is clipped"))
  await model.send(screenshot: {
    captures += 1
    return Data([4, 5, 6])
  })
  #expect(captures == 1)
  #expect(model.attachments == original)
  #expect(try Data(contentsOf: image.url) == Data([1, 2, 3]))
  #expect(try String(contentsOf: diagnostics.url, encoding: .utf8) == log)
  #expect(model.draft == "/feedback The text is clipped")
}

@MainActor @Test(.timeLimit(.minutes(1)))
func feedbackPreservesUserFilesAndReservesTheLastSlotForItsScreenshot() async throws {
  let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: root) }
  let model = ChatModel(
    draftStore: AttachmentStore(directory: root), admit: { _, _, _, _ in false })
  model.connection = .connected
  for index in 0..<(AttachmentLimits.maximumCount - 1) {
    await model.attach(data: Data([1]), name: "Note-\(index).txt", mediaType: "text/plain")
  }
  let original = model.attachments
  model.draft = "/feedback Keep the attached notes"
  await model.send(screenshot: { Data([2]) })
  #expect(Array(model.attachments.dropLast()) == original)
  #expect(model.attachments.count == AttachmentLimits.maximumCount)
  let image = try #require(model.attachments.last)
  #expect(image.reference.name == "Feedback.png")
  #expect(try Data(contentsOf: image.url) == Data([2]))
  #expect(model.draft == "/feedback Keep the attached notes")
}

@MainActor @Test(.timeLimit(.minutes(1)), arguments: [false, true])
func attachmentOnlyAdmissionPreservesFilesAndNewTyping(admitted: Bool) async throws {
  let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: root) }
  let gate = AdmissionGate()
  let model = ChatModel(
    draftStore: AttachmentStore(directory: root),
    admit: { text, _, files, _ in
      #expect(text.isEmpty)
      #expect(files.count == 1)
      return await gate.admit(text)
    })
  model.connection = .connected
  await model.attach(data: Data([1]), name: "First.txt", mediaType: "text/plain")
  let sending = Task { await model.send() }
  for await _ in gate.entered { break }
  model.draft = "New typing"
  await model.attach(data: Data([2]), name: "Second.txt", mediaType: "text/plain")
  await gate.finish(admitted)
  await sending.value
  #expect(
    model.attachments.map(\.reference.name)
      == (admitted ? ["Second.txt"] : ["First.txt", "Second.txt"]))
  #expect(model.draft == "New typing")
  #expect(try Data(contentsOf: #require(model.attachments.last).url) == Data([2]))
}

@MainActor private func connectModelThenRelease(_ session: CoordinatorSession) async throws {
  let model = ChatModel()
  await model.connect(session: session, address: try CoordinatorAddress(host: "100.64.1.2"))
  while model.connection != .connected { try await Task.sleep(for: .milliseconds(1)) }
}

@Test(.timeLimit(.minutes(1)))
func viewLifetimeCancellationAndModelDestructionBothCloseTheirSockets() async throws {
  try await withCoordinator { session, fixture, _ in
    let model = await MainActor.run { ChatModel() }
    await model.connect(session: session, address: try CoordinatorAddress(host: "100.64.1.2"))
    let lifetime = Task { await model.runUntilCancelled() }
    while await model.connection != .connected { try await Task.sleep(for: .milliseconds(1)) }
    lifetime.cancel()
    await lifetime.value
    for await _ in fixture.closedObservations { break }
    #expect(await model.connection == .disconnected)
    #expect(await session.submit("No orphaned observer") == false)
  }
  try await withCoordinator { session, fixture, _ in
    try await connectModelThenRelease(session)
    for await _ in fixture.closedObservations { break }
    #expect(await session.submit("Model was released") == false)
  }
}
