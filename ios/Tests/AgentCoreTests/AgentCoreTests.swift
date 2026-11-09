import CryptoKit
import Foundation
import Network
import Testing

@testable import AgentCore

@Test func rustProducedContractsRoundTripThroughSwift() throws {
  let decoder = JSONDecoder()
  let encoder = JSONEncoder()
  let events = try Data(
    contentsOf: #require(
      Bundle.module.url(forResource: "events", withExtension: "ndjson", subdirectory: "Fixtures")))
  for line in events.split(separator: 10) {
    let event = try decoder.decode(NativeEvent.self, from: Data(line))
    #expect(try decoder.decode(NativeEvent.self, from: encoder.encode(event)) == event)
  }
  let commands = try Data(
    contentsOf: #require(
      Bundle.module.url(forResource: "commands", withExtension: "ndjson", subdirectory: "Fixtures"))
  )
  for line in commands.split(separator: 10) {
    let command = try decoder.decode(NativeCommand.self, from: Data(line))
    #expect(try decoder.decode(NativeCommand.self, from: encoder.encode(command)) == command)
  }
}

@Test func ordinaryNetworksAndAmbiguousAddressesAreRejected() throws {
  for host in [
    "localhost", "127.0.0.1", "192.168.1.2", "100.63.1.2", "100.128.1.2", "100.064.1.2",
    "100.64.1.2.example", "::1",
  ] {
    #expect(throws: ConnectionError.self) { try CoordinatorAddress(host: host) }
  }
  #expect(try CoordinatorAddress(host: "100.64.1.2").port == 43187)
  #expect(throws: ConnectionError.self) { try CoordinatorAddress(host: "100.64.1.2", port: 0) }
}

@Test func reconnectHistoryAndPlacementDoNotDuplicateAnInput() {
  var transcript = Transcript()
  let request = NativeRequestSubmit(
    attachments: nil, inputId: "11111111-1111-4111-8111-111111111111", mode: .steer, text: "Hello")
  transcript.pending(request, delivery: .uncertain)
  let stableID = transcript.rows.first?.id
  let user = HistoryMessage(
    attachments: nil, continuation: false, entryId: "saved-user", finalChunk: true, role: .user,
    text: "Hello",
    tool: nil)
  transcript.history(.init(messages: [user], nextCursor: nil, pageComplete: true))
  transcript.delivery(request.inputId, .saved, entryID: "saved-user")
  transcript.delivery(request.inputId, .uncertain)
  transcript.pending(request, delivery: .uncertain)
  #expect(transcript.rows.first(where: { $0.inputID == request.inputId })?.delivery == .saved)
  transcript.history(.init(messages: [user], nextCursor: nil, pageComplete: true))
  #expect(transcript.rows.count == 1)
  #expect(transcript.rows.first?.id == stableID)
  #expect(transcript.rows.first?.delivery == .saved)
}

@Test func historyReconstructsMultipleBlocksChunksAndToolFailure() {
  var transcript = Transcript()
  let first = HistoryMessage(
    attachments: nil, continuation: false, entryId: "entry", finalChunk: false, role: .commentary,
    text: "First ",
    tool: nil)
  let continuation = HistoryMessage(
    attachments: nil, continuation: true, entryId: "entry", finalChunk: true, role: .commentary,
    text: "block",
    tool: nil)
  let second = HistoryMessage(
    attachments: nil, continuation: false, entryId: "entry", finalChunk: true, role: .thinking,
    text: "Another block",
    tool: nil)
  transcript.history(.init(messages: [first], nextCursor: nil, pageComplete: false))
  #expect(transcript.rows.isEmpty)
  transcript.history(
    .init(messages: [continuation, second], nextCursor: "earlier", pageComplete: true))
  #expect(transcript.rows.map(\.text) == ["First block", "Another block"])
  let ids = transcript.rows.map(\.id)
  transcript.history(
    .init(messages: [first, continuation, second], nextCursor: nil, pageComplete: true))
  #expect(transcript.rows.map(\.id) == ids)
  transcript.activity(.toolStarted(.init(callId: "tool", name: "check")))
  transcript.message(
    .init(
      attachments: nil, continuation: false, entryId: "result", finalChunk: true, role: .tool,
      text: "check failed",
      tool: .init(callId: "tool", name: "check", status: .failed)))
  #expect(transcript.rows.filter { $0.tool?.callId == "tool" }.count == 1)
  #expect(transcript.rows.last?.tool?.status == .failed)
}

@Test func streamedTextSettlesToOneDurableAnswerAndMemoryIsBounded() {
  var transcript = Transcript()
  transcript.response("Hello ")
  transcript.response("world")
  #expect(transcript.rows.last?.text == "Hello world")
  transcript.answer(.init(entryId: "answer", text: "Hello world"))
  transcript.answer(.init(entryId: "answer", text: "Hello world"))
  #expect(transcript.rows.count == 1)
  #expect(transcript.rows.first?.role == .final)
  for index in 0...Transcript.maximumRows { transcript.notice("Notice \(index)") }
  #expect(transcript.rows.count == Transcript.maximumRows)
}

@Test func markdownPreservesCodeAndStructuredBlocks() {
  let blocks = RichText.parse(
    "# Heading\n\nText **bold** and [link](https://example.invalid).\n\n```swift\nlet x = \"🦀\"\n```\n\n> Quote\n\n| A | B |\n|---|---|\n| 1 | 2 |"
  )
  #expect(blocks.count == 5)
  #expect(blocks.contains(.code("let x = \"🦀\"", language: "swift")))
  guard case .table(let rows) = blocks.last else {
    Issue.record("Missing native table")
    return
  }
  #expect(rows.count == 2)
}

private func withLocalPeer(_ body: @Sendable (LineConnection, NWConnection) async throws -> Void)
  async throws
{
  let parameters = NWParameters.tcp
  parameters.requiredLocalEndpoint = .hostPort(host: .ipv4(.loopback), port: .any)
  let listener = try NWListener(using: parameters)
  let ready = AsyncThrowingStream<Void, any Error>.makeStream()
  let accepted = AsyncStream<NWConnection>.makeStream()
  listener.stateUpdateHandler = { state in
    switch state {
    case .ready: ready.continuation.yield(())
    case .failed(let error): ready.continuation.finish(throwing: error)
    case .cancelled: ready.continuation.finish()
    default: break
    }
  }
  listener.newConnectionHandler = { accepted.continuation.yield($0) }
  listener.start(queue: .global())
  defer {
    listener.cancel()
    accepted.continuation.finish()
  }
  for try await _ in ready.stream { break }
  let port = try #require(listener.port)
  let connection = NWConnection(host: .ipv4(.loopback), port: port, using: .tcp)
  let client = LineConnection(connection: connection)
  defer { connection.cancel() }
  for await server in accepted.stream {
    server.start(queue: .global())
    do {
      try await withTaskCancellationHandler {
        try await body(client, server)
      } onCancel: {
        connection.cancel()
        server.cancel()
      }
      await client.close()
      server.cancel()
    } catch {
      await client.close()
      server.cancel()
      throw error
    }
    break
  }
}

@Test(.timeLimit(.minutes(1))) func realNetworkTransportHandlesFragmentedUnicodeAndCancellation()
  async throws
{
  try await withLocalPeer { client, server in
    let event = NativeEvent.notice(.init(text: "Unicode 🦀\u{2028} stays in the frame"))
    var data = try JSONEncoder().encode(event)
    data.append(10)
    try await send(Data(data.prefix(5)), on: server)
    try await send(Data(data.dropFirst(5)), on: server)
    #expect(try await client.next() == event)
    let reading = Task { try await client.next() }
    reading.cancel()
    do {
      _ = try await reading.value
      Issue.record("Cancelled reader returned a message")
    } catch {}
    await client.close()
  }
}

enum UnavailablePeer: CaseIterable, Sendable { case closedTCP, missingUnixSocket }

@Test(.timeLimit(.minutes(1)), arguments: UnavailablePeer.allCases)
func realNetworkTransportUnavailablePeerReleasesItsOriginalFailure(_ peer: UnavailablePeer)
  async throws
{
  let directory = URL(filePath: "/tmp").appending(path: "agent-unavailable-\(UUID().uuidString)")
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: directory) }
  let socket: NWConnection
  let expectedFailures: [NWError]
  switch peer {
  case .closedTCP:
    let listener = try NWListener(using: .tcp, on: .any)
    let listening = AsyncThrowingStream<Void, any Error>.makeStream()
    listener.stateUpdateHandler = { state in
      switch state {
      case .ready: listening.continuation.yield(())
      case .cancelled: listening.continuation.finish()
      case .failed(let error): listening.continuation.finish(throwing: error)
      default: break
      }
    }
    listener.newConnectionHandler = { $0.cancel() }
    listener.start(queue: .global())
    defer { listener.cancel() }
    for try await _ in listening.stream { break }
    let port = try #require(listener.port)
    listener.cancel()
    for try await _ in listening.stream {}
    socket = NWConnection(host: .ipv4(.loopback), port: port, using: .tcp)
    // Network.framework may report the closed listener as refused or reset.
    expectedFailures = [.posix(.ECONNREFUSED), .posix(.ECONNRESET)]
  case .missingUnixSocket:
    socket = NWConnection(
      to: .unix(path: directory.appending(path: "missing.sock").path), using: .tcp)
    expectedFailures = [.posix(.ENOENT)]
  }
  let failures = AsyncStream<NWError>.makeStream(bufferingPolicy: .bufferingNewest(1))
  socket.stateUpdateHandler = { state in
    switch state {
    case .waiting(let error), .failed(let error): failures.continuation.yield(error)
    default: break
    }
  }
  defer { failures.continuation.finish() }
  let client = LineConnection(connection: socket)
  defer { socket.cancel() }
  let sending = Task {
    defer { failures.continuation.finish() }
    do {
      try await client.send(.observe(.init(inputIds: [])))
    } catch {
      // A send may fail before the optional state observer receives the error.
      if let failure = error as? NWError { failures.continuation.yield(failure) }
      throw error
    }
  }
  defer { sending.cancel() }
  // Bound release after either Network.framework or the send reports failure;
  // OS path establishment is outside that response-time contract.
  let original = try #require(await failures.stream.first(where: { _ in true }))
  #expect(expectedFailures.contains(original))
  do {
    try await withThrowingTaskGroup(of: Void.self) { group in
      group.addTask { try await sending.value }
      group.addTask {
        try await Task.sleep(for: .seconds(2))
        throw ConnectionError.timedOut
      }
      defer {
        group.cancelAll()
        sending.cancel()
      }
      try await group.next()
      Issue.record("An unavailable connection accepted input")
    }
  } catch {
    #expect(!(error is ConnectionError), "Network failure must release the send before its timeout")
    #expect(
      error as? NWError == original, "The owner must classify the original network failure")
    #expect(ConnectionError.shouldReconnect(after: error))
  }
  await client.close()
}

@Test(.timeLimit(.minutes(1)))
func realNetworkTransportRejectsOversizedFramesAndClosesTheTransport() async throws {
  try await withLocalPeer { client, server in
    // A peer may still be writing when the frame limit is reached. Awaiting
    // that write before observing rejection can deadlock on TCP backpressure.
    let writing = Task {
      try await send(Data(repeating: 65, count: LineConnection.maximumFrameBytes * 2), on: server)
    }
    defer { server.cancel() }
    do {
      _ = try await client.next()
      Issue.record("Oversized frame accepted")
    } catch let error as ConnectionError { #expect(error == .frameTooLarge) }
    do {
      try await client.send(.interrupt)
      Issue.record("A rejected frame left its connection writable")
    } catch let error as ConnectionError { #expect(error == .closed) }
    await client.close()
    server.cancel()
    // Closing the rejected transport may interrupt the peer's pending write.
    _ = try? await writing.value
  }
}

@Test(.timeLimit(.minutes(1)))
func realNetworkTransportAcceptsAnExactLimitFrameAndTheFollowingFrame() async throws {
  try await withLocalPeer { client, server in
    let encoder = JSONEncoder()
    let overhead = try encoder.encode(NativeEvent.notice(.init(text: ""))).count
    let text = String(repeating: "x", count: LineConnection.maximumFrameBytes - overhead)
    var bytes = try encoder.encode(NativeEvent.notice(.init(text: text)))
    try #require(bytes.count == LineConnection.maximumFrameBytes)
    bytes.append(10)
    bytes.append(try encoder.encode(NativeEvent.idle))
    bytes.append(10)
    let frames = bytes
    let writing = Task { try await send(frames, on: server) }
    defer { server.cancel() }
    guard case .notice(let notice) = try await client.next() else {
      Issue.record("Expected the complete boundary-sized notice")
      return
    }
    #expect(SHA256.hash(data: Data(notice.text.utf8)) == SHA256.hash(data: Data(text.utf8)))
    #expect(try await client.next() == .idle)
    try await writing.value
  }
}

@Test(.timeLimit(.minutes(1)))
func realNetworkTransportDrainsBufferedFramesBeforeRejectingIncompleteEOF() async throws {
  try await withLocalPeer { client, server in
    let events = [NativeEvent.notice(.init(text: "First 🦀")), .idle]
    var data = Data()
    for event in events {
      data.append(try JSONEncoder().encode(event))
      data.append(10)
    }
    data.append(contentsOf: "{\"type\":".utf8)
    try await send(data, on: server)
    try await withCheckedThrowingContinuation {
      (continuation: CheckedContinuation<Void, any Error>) in
      server.send(
        content: nil, contentContext: .finalMessage, isComplete: true,
        completion: .contentProcessed { error in
          if let error { continuation.resume(throwing: error) } else { continuation.resume() }
        })
    }
    for event in events { #expect(try await client.next() == event) }
    do {
      _ = try await client.next()
      Issue.record("Incomplete frame accepted")
    } catch let error as ConnectionError { #expect(error == .incompleteFrame) }
    do {
      try await client.send(.interrupt)
      Issue.record("Incomplete EOF left its connection writable")
    } catch let error as ConnectionError { #expect(error == .closed) }
  }
}

@Test(.timeLimit(.minutes(1)))
func realNetworkTransportClosesEvenWhenTheReaderWasAlreadyCancelled() async throws {
  try await withLocalPeer { client, _ in
    let reading = Task {
      withUnsafeCurrentTask { $0?.cancel() }
      return try await client.next()
    }
    do {
      _ = try await reading.value
      Issue.record("An already-cancelled reader returned a message")
    } catch is CancellationError {}
    do {
      try await client.send(.interrupt)
      Issue.record("Cancelling before receive left its connection writable")
    } catch let error as ConnectionError { #expect(error == .closed) }
  }
}

@Test(.timeLimit(.minutes(1)))
func realNetworkTransportClosesAfterAnInvalidEvent() async throws {
  try await withLocalPeer { client, server in
    try await send(Data("{}\n".utf8), on: server)
    do {
      _ = try await client.next()
      Issue.record("Invalid event accepted")
    } catch is DecodingError {}
    do {
      try await client.send(.interrupt)
      Issue.record("An invalid event left its connection writable")
    } catch let error as ConnectionError { #expect(error == .closed) }
  }
}

@Test(.timeLimit(.minutes(1))) func attachmentsSurviveAcknowledgedDeliveryAndDownloadOnlyOnDemand()
  async throws
{
  try await withCoordinator { session, fixture, outbox in
    let store = AttachmentStore(
      directory: outbox.deletingLastPathComponent().appending(path: "Drafts"))
    let bytes = Data(repeating: 42, count: AttachmentLimits.chunkBytes + 17)
    let file = try await store.stage(data: bytes, name: "Notes.txt", mediaType: "text/plain")
    let address = try CoordinatorAddress(host: "100.64.1.2")
    await session.connect(address)
    _ = try await until(session) { $0.connection == .connected }
    #expect(await session.submit("", files: [file]))
    let acknowledged = try await until(session) { $0.rows.contains { $0.delivery == .saved } }
    #expect(acknowledged.rows.last?.attachments == [file.reference])
    let inputID = try #require(acknowledged.rows.last?.inputID)
    #expect(await fixture.uploadCount() == 2)
    #expect(await fixture.uploaded(file.id) == bytes)
    let saved = try savedInputs(at: outbox)
    #expect(saved.first?.attachments == [file.reference])
    // The pending copy has its own lifetime, independent of the picker/draft copy.
    try await store.remove(file)
    let pendingFile = outbox.deletingLastPathComponent().appending(
      path: "Attachments/\(file.id)/Notes.txt")
    #expect(try Data(contentsOf: pendingFile) == bytes)
    // Uncached previews require the lost-reply connection to recover first.
    _ = try await until(session) { $0.connection == .connected }
    let preview = try await session.preview(file.reference)
    #expect(try Data(contentsOf: preview) == bytes)
    #expect(try await session.preview(file.reference) == preview)
    #expect(await session.submit("/help", files: [file]) == false)
    #expect(await fixture.count() == 1, "A rejected control must not admit or clear its files")
    await session.disconnect()
    await session.connect(address)
    _ = try await until(session) {
      $0.connection == .connected
        && $0.rows.last?.entryID == "placed-\(inputID)"
        && $0.rows.last?.delivery == .saved
    }
    #expect(await fixture.count() == 1)
    #expect(
      try savedInputs(at: outbox).isEmpty)
  }
}

@Test(.timeLimit(.minutes(1))) func feedbackReceiptAcknowledgesTheOriginalDurableInput()
  async throws
{
  try await withCoordinator { session, _, outbox in
    let store = AttachmentStore(
      directory: outbox.deletingLastPathComponent().appending(path: "Drafts"))
    let file = try await store.stage(
      data: Data([1, 2, 3]), name: "Feedback.png", mediaType: "image/png")
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    _ = try await until(session) { $0.connection == .connected }
    #expect(await session.submit("/feedback Fix this", files: [file]))
    let result = try await until(session) {
      $0.rows.contains { $0.text.contains("Repair task: repair-fixture") }
    }
    #expect(result.rows.first(where: { $0.role == .user })?.delivery == .saved)
    #expect(
      try savedInputs(at: outbox).isEmpty)
  }
}

@Test(.timeLimit(.minutes(1))) func rejectedUploadRestoresFilesUnderTheDraftOwnersLifetime()
  async throws
{
  try await withCoordinator { session, fixture, outbox in
    let store = AttachmentStore(
      directory: outbox.deletingLastPathComponent().appending(path: "Drafts"))
    let bytes = Data("Keep this file".utf8)
    let file = try await store.stage(data: bytes, name: "Notes.txt", mediaType: "text/plain")
    await fixture.refuseUploads()
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    _ = try await until(session) { $0.connection == .connected }
    try await fixture.liveMessage(
      .init(
        continuation: false, entryId: "retained-server-message", finalChunk: true, role: .user,
        text: "Fix this"))
    _ = try await until(session) { $0.rows.contains { $0.entryID == "retained-server-message" } }
    #expect(await session.submit("Fix this", files: [file]))
    let rejected = try await until(session) { $0.rows.last?.delivery == .rejected }
    let id = try #require(rejected.rows.last?.inputID)
    #expect(await fixture.count() == 0, "Upload refusal must happen before input admission")
    try await store.remove(file)
    let restored = try #require(await session.restore(id, to: store))
    #expect(restored.text == "Fix this")
    let restoredFile = try #require(restored.attachments.first)
    #expect(restoredFile.id != file.id)
    #expect(
      restoredFile.url.deletingLastPathComponent().deletingLastPathComponent()
        == file.url.deletingLastPathComponent().deletingLastPathComponent())
    #expect(try Data(contentsOf: restoredFile.url) == bytes)
    let draftURL = outbox.deletingLastPathComponent().appending(path: "composer.json")
    let persistence = ComposerDraftStore(url: draftURL)
    let empty = try await persistence.load(from: store)
    try await persistence.save(
      .init(text: restored.text, mode: restored.mode, attachments: restored.attachments),
      revision: empty.revision + 1)
    #expect(await session.commitRestoration(restored) == .committed)
    let retired = try await until(session) { _ in true }
    #expect(!retired.rows.contains { $0.inputID == id })
    #expect(
      retired.rows.contains { $0.entryID == "retained-server-message" && $0.text == "Fix this" })
    #expect(await session.restore(id, to: store) == nil)
    let reopened = try await ComposerDraftStore(url: draftURL).load(from: store)
    #expect(reopened.draft.text == "Fix this")
    #expect(reopened.draft.attachments == restored.attachments)
    try await store.remove(restoredFile)
    #expect(!FileManager.default.fileExists(atPath: restoredFile.url.path))
    #expect(
      try savedInputs(at: outbox).isEmpty)
  }
}

@Test(.timeLimit(.minutes(1)))
func restorationPreparationKeepsTheOriginalOutboxUntilTheReplacementIsDurable() async throws {
  try await withCoordinator { session, fixture, outbox in
    let store = AttachmentStore(
      directory: outbox.deletingLastPathComponent().appending(path: "Drafts"))
    let file = try await store.stage(
      data: Data("Original bytes".utf8), name: "Original.txt", mediaType: "text/plain")
    await fixture.refuseUploads()
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    _ = try await until(session) { $0.connection == .connected }
    #expect(await session.submit("Restore this original", files: [file]))
    let rejected = try await until(session) { $0.rows.last?.delivery == .rejected }
    let id = try #require(rejected.rows.last?.inputID)
    let original = try Data(contentsOf: outbox)
    let prepared = try #require(await session.restore(id, to: store))
    #expect(prepared.text == "Restore this original")
    #expect(try Data(contentsOf: outbox) == original)
    let restoredFile = try #require(prepared.attachments.first)
    #expect(try Data(contentsOf: restoredFile.url) == Data("Original bytes".utf8))
    // Preparation alone cannot forget the sole durable record, including if
    // saving the replacement fails or the caller exits before saving it.
    await session.disconnect()
    #expect(try Data(contentsOf: outbox) == original)
  }
}

@Test(.timeLimit(.minutes(1)))
func failedRestorationRemovesPartialCopiesAndPreservesTheOriginalOutbox() async throws {
  try await withCoordinator { session, fixture, outbox in
    let draftDirectory = outbox.deletingLastPathComponent().appending(path: "Drafts")
    let store = AttachmentStore(directory: draftDirectory)
    let first = try await store.stage(
      data: Data("First".utf8), name: "First.txt", mediaType: "text/plain")
    let second = try await store.stage(
      data: Data("Second".utf8), name: "Second.txt", mediaType: "text/plain")
    await fixture.refuseUploads()
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    _ = try await until(session) { $0.connection == .connected }
    #expect(await session.submit("Keep the original", files: [first, second]))
    let rejected = try await until(session) { $0.rows.last?.delivery == .rejected }
    let id = try #require(rejected.rows.last?.inputID)
    let original = try Data(contentsOf: outbox)
    try await store.remove(first)
    try await store.remove(second)
    let missingSource = outbox.deletingLastPathComponent().appending(
      path: "Attachments/\(second.id)/\(second.reference.name)")
    try FileManager.default.removeItem(at: missingSource)
    #expect(await session.restore(id, to: store) == nil)
    #expect(try FileManager.default.contentsOfDirectory(atPath: draftDirectory.path).isEmpty)
    #expect(try Data(contentsOf: outbox) == original)
  }
}

@Test(.timeLimit(.minutes(1)))
func failedRestorationRetirementKeepsTheSavedReplacementAndAllowsRetry() async throws {
  try await withCoordinator { session, fixture, outbox in
    let directory = outbox.deletingLastPathComponent()
    let store = AttachmentStore(directory: directory.appending(path: "Drafts"))
    let bytes = Data("Keep the saved replacement".utf8)
    let file = try await store.stage(data: bytes, name: "Original.txt", mediaType: "text/plain")
    await fixture.refuseUploads()
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    _ = try await until(session) { $0.connection == .connected }
    #expect(await session.submit("Keep this draft", files: [file]))
    let rejected = try await until(session) { $0.rows.last?.delivery == .rejected }
    let inputID = try #require(rejected.rows.last?.inputID)
    let prepared = try #require(await session.restore(inputID, to: store))
    let original = try Data(contentsOf: outbox)
    let draftURL = directory.appending(path: "composer.json")
    let persistence = ComposerDraftStore(url: draftURL)
    let empty = try await persistence.load(from: store)
    try await persistence.save(
      .init(text: prepared.text, mode: prepared.mode, attachments: prepared.attachments),
      revision: empty.revision + 1)
    let stranger = CoordinatorSession(outboxURL: directory.appending(path: "unrelated.json"))
    #expect(await stranger.commitRestoration(prepared) == .invalidated)
    #expect(try Data(contentsOf: outbox) == original)
    try FileManager.default.setAttributes([.posixPermissions: 0o500], ofItemAtPath: directory.path)
    defer {
      try? FileManager.default.setAttributes(
        [.posixPermissions: 0o700], ofItemAtPath: directory.path)
    }
    #expect(await session.commitRestoration(prepared) == .originalRetained)
    let retained = try await until(session) { _ in true }
    let recovery = try #require(retained.rows.first { $0.inputID == inputID })
    #expect(recovery.delivery == .rejected)
    #expect(try Data(contentsOf: outbox) == original)
    let saved = try await ComposerDraftStore(url: draftURL).load(from: store)
    #expect(saved.draft.text == "Keep this draft")
    #expect(saved.draft.attachments == prepared.attachments)
    let ownedFile = try #require(prepared.attachments.first)
    #expect(try Data(contentsOf: ownedFile.url) == bytes)
    try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: directory.path)
    #expect(await session.commitRestoration(prepared) == .committed)
    let retired = try await until(session) { _ in true }
    #expect(!retired.rows.contains { $0.inputID == inputID })
    #expect(
      try savedInputs(at: outbox).isEmpty)
    #expect(try Data(contentsOf: ownedFile.url) == bytes)
    #expect(await fixture.count() == 0, "Retirement never submits the restored draft")
  }
}

@Test(.timeLimit(.minutes(1)))
func reconciledInputInvalidatesRestorationWithoutDeletingTheSavedReplacement() async throws {
  try await withCoordinator { session, fixture, outbox in
    let directory = outbox.deletingLastPathComponent()
    let store = AttachmentStore(directory: directory.appending(path: "Drafts"))
    let bytes = Data("Original attachment".utf8)
    let file = try await store.stage(data: bytes, name: "Original.txt", mediaType: "text/plain")
    await fixture.loseInputAcknowledgements()
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    _ = try await until(session) { $0.connection == .connected }
    #expect(await session.submit("Original accepted by the coordinator", files: [file]))
    let uncertain = try await until(session) { $0.rows.last?.delivery == .uncertain }
    let inputID = try #require(uncertain.rows.last?.inputID)
    let prepared = try #require(await session.restore(inputID, to: store))
    let draftURL = directory.appending(path: "composer.json")
    let persistence = ComposerDraftStore(url: draftURL)
    let empty = try await persistence.load(from: store)
    try await persistence.save(
      .init(text: prepared.text, mode: prepared.mode, attachments: prepared.attachments),
      revision: empty.revision + 1)
    _ = try await until(session) { $0.connection == .connected && $0.rows.last?.entryID != nil }
    #expect(await session.commitRestoration(prepared) == .invalidated)
    #expect(await session.restore(inputID, to: store) == nil)
    #expect(
      try savedInputs(at: outbox).isEmpty)
    let saved = try await ComposerDraftStore(url: draftURL).load(from: store)
    #expect(saved.draft.attachments == prepared.attachments)
    #expect(try Data(contentsOf: #require(prepared.attachments.first).url) == bytes)
    #expect(await fixture.count() == 1, "Reconciliation never replays an accepted input")
  }
}

private func until(_ session: CoordinatorSession, _ predicate: @Sendable (ChatSnapshot) -> Bool)
  async throws -> ChatSnapshot
{
  for await snapshot in session.updates where predicate(snapshot) { return snapshot }
  throw ConnectionError.closed
}

@Test(.timeLimit(.minutes(1))) func sessionPersistsUncertainInputWithoutReplay()
  async throws
{
  try await withCoordinator { session, fixture, outbox in
    let address = try CoordinatorAddress(host: "100.64.1.2")
    await fixture.loseInputAcknowledgements()
    await session.connect(address)
    _ = try await until(session) { $0.connection == .connected }
    #expect(await session.submit("Keep this original") == true)
    let uncertain = try await until(session) { $0.rows.contains { $0.delivery == .uncertain } }
    let id = try #require(uncertain.rows.first(where: { $0.delivery == .uncertain })?.inputID)
    let saved = try savedInputs(at: outbox)
    #expect(saved.map(\.inputId) == [id])
    #expect(await fixture.count() == 1, "An uncertain write must not be replayed automatically")
  }
}

@Test func interleavedHistoryPagesHaveIndependentAssemblyAndRemainChronological() {
  var transcript = Transcript()
  let action = UUID()
  let observerFirst = HistoryMessage(
    attachments: nil, continuation: false, entryId: "current", finalChunk: false, role: .final,
    text: "Current ",
    tool: nil)
  let observerLast = HistoryMessage(
    attachments: nil, continuation: true, entryId: "current", finalChunk: true, role: .final,
    text: "answer",
    tool: nil)
  let actionFirst = HistoryMessage(
    attachments: nil, continuation: false, entryId: "older", finalChunk: false, role: .user,
    text: "Older ", tool: nil
  )
  let actionLast = HistoryMessage(
    attachments: nil, continuation: true, entryId: "older", finalChunk: true, role: .user,
    text: "question", tool: nil
  )
  transcript.history(.init(messages: [observerFirst], nextCursor: nil, pageComplete: false))
  transcript.history(
    .init(messages: [actionFirst], nextCursor: nil, pageComplete: false), olderPage: true,
    requestID: action)
  transcript.history(.init(messages: [observerLast], nextCursor: "previous", pageComplete: true))
  #expect(transcript.rows.map(\.text) == ["Current answer"])
  transcript.history(
    .init(messages: [actionLast], nextCursor: "oldest", pageComplete: true), olderPage: true,
    requestID: action)
  #expect(transcript.rows.map(\.text) == ["Older question", "Current answer"])
  transcript.reconnect()
  transcript.history(
    .init(messages: [observerFirst, observerLast], nextCursor: nil, pageComplete: true))
  #expect(transcript.rows.map(\.text) == ["Older question", "Current answer"])
}

@Test func durableLiveNoticeDeduplicatesAgainstReconnectHistory() {
  var transcript = Transcript()
  let notice = HistoryMessage(
    attachments: nil, continuation: false, entryId: "durable-notice", finalChunk: true,
    role: .notice,
    text: "A useful update", tool: nil)
  transcript.message(notice)
  let stableID = transcript.rows.first?.id
  transcript.history(.init(messages: [notice], nextCursor: nil, pageComplete: true))
  transcript.message(notice)
  #expect(transcript.rows.count == 1)
  #expect(transcript.rows.first?.id == stableID)
}

@Test(.timeLimit(.minutes(1))) func readyDoesNotAdmitInputBeforeTheInitialSnapshotCompletes()
  async throws
{
  try await withCoordinator { session, fixture, _ in
    await fixture.holdSnapshot()
    await session.connect(try CoordinatorAddress(host: "100.64.1.2"))
    _ = try await until(session) { $0.conversationID != nil && $0.connection == .connecting }
    #expect(await session.submit("Too early") == false)
    await fixture.releaseSnapshot()
    _ = try await until(session) { $0.connection == .connected }
    #expect(await fixture.count() == 0)
  }
}
