import AgentCore
import CryptoKit
import Foundation
import OSLog
import Observation

#if os(iOS)
  import PhotosUI
  import SwiftUI
#endif

@MainActor @Observable
final class RowModel: Identifiable {
  let id: String
  var row: ChatRow
  init(_ row: ChatRow) {
    id = row.id
    self.row = row
  }
}

@MainActor @Observable
final class ChatModel {
  private enum ComposerEvent: String {
    case active, inactive, restored, edited, sendRequested, sendIgnored, cleared, outboxSaved,
      localDraftRestored,
      saveFailed
  }

  @ObservationIgnored private let composerLogger = Logger(
    subsystem: "dev.jessfraz.Agent", category: "Composer")
  private enum Admission {
    case pending(text: String)
    case saved
  }

  var rows: [RowModel] = [] {
    didSet { transcriptItems = TranscriptItem.grouped(rows, preserving: transcriptItems) }
  }
  private(set) var transcriptItems: [TranscriptItem] = []
  var connection: ConnectionState = .disconnected
  var model = "Unknown"
  var commands: [SlashCommand] = []
  private(set) var settingsSupported = false
  private(set) var workInspectionSupported = false
  let inspectedConversation: SavedConversation?
  private(set) var conversationSettings: NativeSettingsSnapshot?
  private(set) var settingsBusy = false
  var settingsError: String?
  @ObservationIgnored private var settingsRequest: UUID?
  var busy = false
  var canLoadEarlier = false
  var loadingHistory = false
  var includesLatest = true
  var sourceWarning: String?
  var sources: [SourceState] = []
  var error: String?
  var revision = 0
  var draft = "" {
    didSet {
      draftRevision &+= 1
      scheduleDraftSave()
      if !changingDraft, oldValue.isEmpty != draft.isEmpty { logComposer(.edited) }
    }
  }
  @ObservationIgnored private var draftRevision = 0
  var attachments: [LocalAttachment] = [] { didSet { scheduleDraftSave() } }
  var previewURL: URL?
  private(set) var preparingAttachments = false
  var mode: InputMode = .steer { didSet { scheduleDraftSave() } }
  private(set) var draftReady: Bool
  private(set) var pendingSharedImport = false
  var draftError: String?
  @ObservationIgnored private let persistence: ComposerDraftStore?
  @ObservationIgnored private let sharedInbox: SharedDraftInbox?
  @ObservationIgnored private var persistentRevision: UInt64 = 0
  @ObservationIgnored private var consumedTransfers: Set<UUID> = []
  @ObservationIgnored private var changingDraft = false
  @ObservationIgnored private var restoringDraft = false
  private var admission: Admission?
  var admitting: Bool { admission != nil }
  @ObservationIgnored private var generation = UUID()
  @ObservationIgnored private var admit:
    @Sendable (String, InputMode, [LocalAttachment]) async -> Bool
  @ObservationIgnored private var draftStore: AttachmentStore?
  @ObservationIgnored private var feedbackScreenshotID: String? { didSet { scheduleDraftSave() } }

  init(
    inspecting conversation: SavedConversation? = nil,
    draftStore: AttachmentStore? = nil, persistence: ComposerDraftStore? = nil,
    sharedInbox: SharedDraftInbox? = nil,
    admit: @escaping @Sendable (String, InputMode, [LocalAttachment]) async -> Bool = { _, _, _ in
      false
    }
  ) {
    inspectedConversation = conversation
    self.draftStore = draftStore
    self.persistence = persistence
    self.sharedInbox = sharedInbox
    draftReady = persistence == nil
    self.admit = admit
  }
  deinit {
    observation?.cancel()
    let ownedSession = session
    Task { await ownedSession?.disconnect() }
  }
  @ObservationIgnored private var session: CoordinatorSession?
  @ObservationIgnored private var observation: Task<Void, Never>?
  @ObservationIgnored private var address: CoordinatorAddress?
  @ObservationIgnored private var active = true

  static func persistent(
    in directory: URL = .applicationSupportDirectory.appending(path: "Agent")
  ) -> ChatModel {
    ChatModel(
      draftStore: AttachmentStore(directory: directory.appending(path: "Drafts")),
      persistence: ComposerDraftStore(url: directory.appending(path: "Composer/draft.json")),
      sharedInbox: try? SharedDraftInbox.configured())
  }

  func insertCommand(_ command: SlashCommand) {
    let name = command.usage.prefix(while: { !$0.isWhitespace })
    guard draftReady, !name.isEmpty else { return }
    let text = draft.drop(while: \.isWhitespace)
    let leading = text.prefix(while: { !$0.isWhitespace })
    if commands.contains(where: { $0.usage.prefix(while: { !$0.isWhitespace }) == leading }) {
      let note = text.dropFirst(leading.count)
      draft = String(name) + (note.isEmpty ? " " : String(note))
    } else {
      draft = String(name) + " " + draft
    }
  }

  private var currentDraft: ComposerDraft {
    var text = draft
    if case .pending(let submitted) = admission, !submitted.isEmpty {
      // A crash before outbox admission must recover both unsent notes.
      text = submitted + (text.isEmpty ? "" : "\n\n" + text)
    }
    return ComposerDraft(
      text: text, mode: mode, attachments: attachments, consumedTransfers: consumedTransfers,
      feedbackScreenshotID: feedbackScreenshotID)
  }
  private func scheduleDraftSave() {
    guard draftReady, !changingDraft, let persistence else { return }
    persistentRevision &+= 1
    let revision = persistentRevision
    let value = currentDraft
    Task { [weak self] in
      do { try await persistence.save(value, revision: revision) } catch {
        if self?.persistentRevision == revision { self?.draftError = error.localizedDescription }
      }
    }
  }
  private func persistDraft() async throws {
    guard let persistence else { return }
    guard draftReady else { throw SharedDraftError.busy }
    persistentRevision &+= 1
    try await persistence.save(currentDraft, revision: persistentRevision)
  }
  @discardableResult func saveDraft() async -> Bool {
    do {
      try await persistDraft()
      draftError = nil
      return true
    } catch {
      draftError = error.localizedDescription
      logComposer(.saveFailed)
      return false
    }
  }
  func restoreDraft() async {
    guard !draftReady, !restoringDraft, let persistence else { return }
    restoringDraft = true
    defer { restoringDraft = false }
    let revision = draftRevision
    do {
      let restored = try await persistence.load(from: attachmentStore())
      guard draftRevision == revision, draft.isEmpty, attachments.isEmpty else {
        throw AttachmentError.unavailable(
          "The saved draft was preserved. Clear the current composer before restoring it.")
      }
      changingDraft = true
      draft = restored.draft.text
      mode = restored.draft.mode
      attachments = restored.draft.attachments
      consumedTransfers = restored.draft.consumedTransfers
      feedbackScreenshotID = restored.draft.feedbackScreenshotID
      persistentRevision = restored.revision
      changingDraft = false
      draftReady = true
      logComposer(.restored)
      pendingSharedImport = !consumedTransfers.isEmpty
      _ = await saveDraft()
      guard sharedInbox != nil || consumedTransfers.isEmpty else {
        throw SharedDraftError.notConfigured
      }
      await receiveSharedDrafts()
    } catch { draftError = error.localizedDescription }
  }
  func receiveSharedDrafts() async {
    guard draftReady, !preparingAttachments, !admitting, let sharedInbox, persistence != nil else {
      return
    }
    preparingAttachments = true
    defer { preparingAttachments = false }
    do {
      let store = try attachmentStore()
      try await retireAcknowledgedTransfers(in: sharedInbox)
      // One activation must not drain an unbounded queue or overfill the composer.
      for _ in 0..<AttachmentLimits.maximumCount {
        let consumed = try await sharedInbox.consumeNext(into: store) { [self] batch in
          try await adoptSharedDraft(batch)
        }
        guard consumed else { break }
        // Only acknowledged IDs may leave the durable host ledger. A crash
        // before this cleanup is harmless and is reconciled on next launch.
        try await retireAcknowledgedTransfers(in: sharedInbox)
      }
      draftError = nil
    } catch {
      if !(error is CancellationError) { draftError = error.localizedDescription }
    }
  }
  private func retireAcknowledgedTransfers(in inbox: SharedDraftInbox) async throws {
    let acknowledged = consumedTransfers.filter { inbox.acknowledged($0) }
    consumedTransfers.subtract(acknowledged)
    pendingSharedImport = !consumedTransfers.isEmpty
    if !acknowledged.isEmpty { try await persistDraft() }
  }
  private func adoptSharedDraft(_ batch: SharedDraftBatch) async throws {
    if !consumedTransfers.contains(batch.id) {
      guard batch.attachments.count <= AttachmentLimits.maximumCount - attachments.count else {
        throw AttachmentError.tooMany
      }
      changingDraft = true
      if !batch.note.isEmpty { draft += (draft.isEmpty ? "" : "\n\n") + batch.note }
      attachments.append(contentsOf: batch.attachments)
      consumedTransfers.insert(batch.id)
      changingDraft = false
    }
    pendingSharedImport = true
    // This complete atomic record, including the UUID, must reach disk before
    // SharedDraftInbox is allowed to acknowledge or delete the source batch.
    try await persistDraft()
  }
  func connect(host: String, port: String) async {
    do {
      guard let port = UInt16(port) else { throw ConnectionError.invalidAddress }
      let address = try CoordinatorAddress(
        host: host.trimmingCharacters(in: .whitespacesAndNewlines), port: port)
      let generation = UUID()
      await stop(generation: generation)
      guard self.generation == generation, !Task.isCancelled else { return }
      self.address = address
      let key = SHA256.hash(data: Data("\(address.host):\(address.port)".utf8)).map {
        String(format: "%02x", $0)
      }.joined()
      let directory = try FileManager.default.url(
        for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
      let session = CoordinatorSession(
        outboxURL: directory.appending(path: "Agent/Outbox/\(key)/pending.json"),
        inspecting: inspectedConversation)
      guard self.generation == generation else { return }
      await connect(session: session, address: address)
    } catch { self.error = error.localizedDescription }
  }
  func connect(session: CoordinatorSession, address: CoordinatorAddress) async {
    self.address = address
    self.session = session
    admit = { text, mode, files in await session.submit(text, mode: mode, files: files) }
    rows = []
    error = nil
    observation = Task { [weak self] in
      for await update in session.updates {
        guard !Task.isCancelled else { return }
        self?.apply(update)
      }
    }
    if active { await session.connect(address) }
  }
  func connectInspection(using owner: ChatModel) async {
    guard inspectedConversation != nil, let address = owner.address else {
      error = ConnectionError.closed.localizedDescription
      return
    }
    await connect(host: address.host, port: String(address.port))
  }
  func jobs(_ id: String? = nil) async throws -> NativeJobStatus {
    guard let session else { throw ConnectionError.closed }
    return try await session.jobs(id)
  }
  func retryJob(_ id: String) async throws -> ScheduledJobStatus {
    guard let session else { throw ConnectionError.closed }
    return try await session.retryJob(id)
  }
  func conversations(query: String = "", cursor: String? = nil) async throws
    -> NativeEventConversations
  {
    guard let session else { throw ConnectionError.closed }
    return try await session.conversations(query: query, cursor: cursor)
  }
  func runUntilCancelled() async {
    let lifetime = AsyncStream<Void> { _ in }
    for await _ in lifetime {}
    await stop()
  }
  func stop() async { await stop(generation: UUID()) }
  private func stop(generation: UUID) async {
    self.generation = generation
    observation?.cancel()
    observation = nil
    let ownedSession = session
    session = nil
    connection = .disconnected
    settingsSupported = false
    workInspectionSupported = false
    conversationSettings = nil
    settingsRequest = nil
    settingsBusy = false
    settingsError = nil
    admit = { _, _, _ in false }
    await ownedSession?.disconnect()
  }
  func setActive(_ active: Bool) async {
    self.active = active
    logComposer(active ? .active : .inactive)
    guard let session, let address else { return }
    if active { await session.connect(address) } else { await session.disconnect() }
  }
  func connectNotifications(_ registration: NotificationRegistration) async {
    guard let session else { return }
    await registration.connect { request in try await session.notifications(request) }
  }
  var canSend: Bool {
    draftReady && !pendingSharedImport && !admitting && !preparingAttachments
      && connection == .connected
      && (!draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || !attachments.isEmpty)
  }
  func send(screenshot: (@MainActor () throws -> Data)? = nil) async {
    logComposer(.sendRequested)
    guard canSend
    else {
      logComposer(.sendIgnored)
      return
    }
    let text = draft
    let generation = generation
    var admitted = false
    // Clear the actual bound value before any await. The pending admission
    // retains the recovery copy until the durable outbox takes ownership.
    admission = .pending(text: text)
    changingDraft = true
    draft = ""
    changingDraft = false
    logComposer(.cleared)
    defer {
      admission = nil
      if !admitted, !text.isEmpty {
        // Local rejection has no outbox row. Preserve both the original and
        // anything typed while admission or attachment preparation was pending.
        draft = text + (draft.isEmpty ? "" : "\n\n" + draft)
        logComposer(.localDraftRestored)
      }
    }
    let mode = mode
    let admit = admit
    var files = attachments
    do {
      if text.split(whereSeparator: \.isWhitespace).first == "/feedback",
        feedbackScreenshotID == nil, let screenshot
      {
        guard files.count < AttachmentLimits.maximumCount else { throw AttachmentError.tooMany }
        // Capture once. A failed admission retains this exact image.
        let data = try screenshot()
        let file = try await attachmentStore().stage(
          data: data, name: "Feedback.png", mediaType: "image/png")
        guard self.generation == generation else {
          try? await draftStore?.remove(file)
          return
        }
        guard attachments.count < AttachmentLimits.maximumCount else {
          try? await draftStore?.remove(file)
          throw AttachmentError.tooMany
        }
        attachments.append(file)
        files.append(file)
        feedbackScreenshotID = file.id
        // Feedback owns private evidence, not transcript messages. Preserve the
        // screenshot and user files even when log access or staging fails.
        if attachments.count < AttachmentLimits.maximumCount {
          let diagnostics = await ComposerDiagnostics.export()
          if let log = try? await attachmentStore().stage(
            data: Data(diagnostics.text.utf8), name: "Composer.log", mediaType: "text/plain")
          {
            if self.generation == generation, attachments.count < AttachmentLimits.maximumCount {
              attachments.append(log)
              files.append(log)
            } else {
              try? await draftStore?.remove(log)
            }
          }
        }
      }
    } catch {
      self.error = error.localizedDescription
      return
    }
    guard self.generation == generation else { return }
    admitted = await admit(text, mode, files)
    if admitted {
      // The outbox owns this draft even if its connection has since stopped.
      admission = .saved
      logComposer(.outboxSaved)
      let sentIDs = Set(files.map(\.id))
      attachments.removeAll { sentIDs.contains($0.id) }
      if let id = feedbackScreenshotID, sentIDs.contains(id) { feedbackScreenshotID = nil }
      if await saveDraft(), let store = draftStore {
        for file in files { try? await store.remove(file) }
      }
    }
  }
  private func logComposer(_ event: ComposerEvent) {
    let build = Bundle.main.object(forInfoDictionaryKey: "CFBundleVersion") as? String ?? "unknown"
    // Only state and counts enter system logs; never draft text, files or addresses.
    composerLogger.notice(
      "event=\(event.rawValue, privacy: .public) build=\(build, privacy: .public) generation=\(self.generation.uuidString, privacy: .public) revision=\(self.draftRevision) characters=\(self.draft.count) attachments=\(self.attachments.count) active=\(self.active) ready=\(self.draftReady) connected=\(self.connection == .connected) busy=\(self.busy) admitting=\(self.admitting) preparing=\(self.preparingAttachments) shared=\(self.pendingSharedImport)"
    )
  }
  func restore(_ id: String) async {
    guard draftReady else {
      draftError = draftError ?? "Restore the saved draft before replacing the composer."
      return
    }
    guard !admitting, !preparingAttachments, draft.isEmpty, attachments.isEmpty else {
      error = "Send or clear the current draft before restoring another message."
      return
    }
    guard let session else { return }
    preparingAttachments = true
    defer { preparingAttachments = false }
    do {
      let store = try attachmentStore()
      guard let restored = await session.restore(id, to: store) else { return }
      let previous = currentDraft
      let feedback = restored.text.split(whereSeparator: \.isWhitespace).first == "/feedback"
      replaceComposer(
        with: ComposerDraft(
          text: restored.text + (draft.isEmpty ? "" : "\n\n" + draft), mode: restored.mode,
          attachments: restored.attachments + attachments,
          consumedTransfers: consumedTransfers,
          feedbackScreenshotID: feedback
            ? restored.attachments.last(where: { $0.reference.name == "Feedback.png" })?.id
            : feedbackScreenshotID))
      let savedRevision = persistentRevision &+ 1
      guard await saveDraft() else { return }
      switch await session.commitRestoration(restored) {
      case .committed, .originalRetained:
        break
      case .invalidated:
        guard persistentRevision == savedRevision else {
          error = "Already saved or restored. Review the kept draft before sending."
          return
        }
        replaceComposer(with: previous)
        // Until rollback is durable, the saved replacement still owns its files.
        if await saveDraft() {
          for file in restored.attachments { try await store.remove(file) }
        }
      }
    } catch { self.error = error.localizedDescription }
  }
  private func replaceComposer(with value: ComposerDraft) {
    changingDraft = true
    draft = value.text
    mode = value.mode
    attachments = value.attachments
    consumedTransfers = value.consumedTransfers
    feedbackScreenshotID = value.feedbackScreenshotID
    changingDraft = false
  }
  private func attachmentStore() throws -> AttachmentStore {
    if let draftStore { return draftStore }
    let directory = try FileManager.default.url(
      for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
    let store = AttachmentStore(directory: directory.appending(path: "Agent/Drafts"))
    draftStore = store
    return store
  }
  func attach(urls: [URL]) async {
    guard draftReady, !preparingAttachments else { return }
    preparingAttachments = true
    defer { preparingAttachments = false }
    do {
      let store = try attachmentStore()
      for url in urls {
        guard attachments.count < AttachmentLimits.maximumCount else {
          throw AttachmentError.tooMany
        }
        let scoped = url.startAccessingSecurityScopedResource()
        defer { if scoped { url.stopAccessingSecurityScopedResource() } }
        attachments.append(try await store.stage(url: url))
      }
    } catch { self.error = error.localizedDescription }
  }
  func attach(providers: [NSItemProvider]) async {
    guard draftReady, !preparingAttachments else { return }
    preparingAttachments = true
    defer { preparingAttachments = false }
    var staged: [LocalAttachment] = []
    do {
      guard providers.count <= AttachmentLimits.maximumCount - attachments.count else {
        throw AttachmentError.tooMany
      }
      let images = try await ItemProviderImages.load(providers)
      let store = try attachmentStore()
      for image in images {
        try Task.checkCancellation()
        staged.append(
          try await store.stage(data: image.data, name: image.name, mediaType: image.mediaType))
      }
      try Task.checkCancellation()
      guard staged.count <= AttachmentLimits.maximumCount - attachments.count else {
        throw AttachmentError.tooMany
      }
      // Adopt the complete selection once. Neither the note nor existing files
      // belong to this import, including when a provider fails or is cancelled.
      attachments.append(contentsOf: staged)
    } catch {
      for file in staged { try? await draftStore?.remove(file) }
      if !(error is CancellationError) { self.error = error.localizedDescription }
    }
  }
  func attach(data: Data, name: String, mediaType: String) async {
    guard draftReady else { return }
    do {
      guard attachments.count < AttachmentLimits.maximumCount else { throw AttachmentError.tooMany }
      let file = try await attachmentStore().stage(data: data, name: name, mediaType: mediaType)
      guard attachments.count < AttachmentLimits.maximumCount else {
        try? await draftStore?.remove(file)
        throw AttachmentError.tooMany
      }
      attachments.append(file)
    } catch { self.error = error.localizedDescription }
  }
  #if os(iOS)
    func attach(photo: PhotosPickerItem) async {
      guard draftReady, !preparingAttachments else { return }
      preparingAttachments = true
      defer { preparingAttachments = false }
      do {
        guard let image = try await photo.loadTransferable(type: ImportedPhoto.self) else {
          throw AttachmentError.invalidFile
        }
        await attach(data: image.data, name: "Photo.jpg", mediaType: "image/jpeg")
      } catch { self.error = error.localizedDescription }
    }
  #endif
  func removeAttachment(_ file: LocalAttachment) async {
    guard !admitting, !preparingAttachments else { return }
    attachments.removeAll { $0.id == file.id }
    if feedbackScreenshotID == file.id { feedbackScreenshotID = nil }
    if await saveDraft() { try? await draftStore?.remove(file) }
  }
  func preview(_ reference: AttachmentRef) async {
    do { previewURL = try await session?.preview(reference) } catch {
      self.error = error.localizedDescription
    }
  }
  func refreshSettings(change: ConversationChange? = nil) async {
    guard !settingsBusy, settingsSupported, let session, connection == .connected else { return }
    let id = UUID()
    let generation = generation
    settingsRequest = id
    settingsBusy = true
    settingsError = nil
    defer {
      if settingsRequest == id {
        settingsRequest = nil
        settingsBusy = false
      }
    }
    do {
      let settings = try await session.settings(change: change)
      guard self.generation == generation, !Task.isCancelled else { return }
      conversationSettings = settings
    } catch {
      if self.generation == generation, !(error is CancellationError) {
        settingsError = error.localizedDescription
      }
    }
  }
  func retry(_ id: String) async { await session?.retry(id) }
  func toolDetails(for row: ChatRow) async throws -> ToolDetails {
    guard let entryID = row.entryID, let tool = row.tool, let session else {
      throw ConnectionError.closed
    }
    return try await session.toolDetails(entryID: entryID, callID: tool.callId)
  }
  func history(anchorID: String?) async { await session?.loadEarlier(anchorID: anchorID) }
  func latestHistory() async { await session?.loadLatest() }
  func interrupt() async { await session?.interrupt() }
  private func apply(_ update: ChatSnapshot) {
    let existing = Dictionary(
      rows.map { ($0.id, $0) }, uniquingKeysWith: { original, _ in original })
    let next = update.rows.map { row -> RowModel in
      if let model = existing[row.id] {
        if model.row != row { model.row = row }
        return model
      }
      return RowModel(row)
    }
    if rows.map(\.id) != next.map(\.id) {
      rows = next
    } else {
      transcriptItems = TranscriptItem.grouped(rows, preserving: transcriptItems)
    }
    revision &+= 1
    connection = update.connection
    model = update.model
    busy = update.busy
    commands = update.commands
    settingsSupported = update.settingsSupported
    workInspectionSupported = update.workInspectionSupported
    conversationSettings = update.settings
    canLoadEarlier = update.olderCursor != nil
    loadingHistory = update.loadingHistory
    includesLatest = update.includesLatest
    sourceWarning = update.sourceWarning
    sources = update.sources
    error = update.error
  }
}
