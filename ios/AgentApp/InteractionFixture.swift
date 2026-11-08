#if DEBUG
  import Foundation
  import Network
  import SwiftUI

  @testable import AgentCore
  @testable import AgentUI

  // UI automation supplies only a loopback peer. The real screen, composer,
  // session and durable outbox remain in use; release builds have no override.
  struct InteractionFixture: View {
    @MainActor private struct Lifetime {
      let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
      let model: ChatModel

      init() { model = ChatModel.persistent(in: directory) }
    }

    let port: NWEndpoint.Port
    // View reconstruction must retain the model and its store directory together.
    @State private var lifetime = Lifetime()

    static func fromEnvironment() -> Self? {
      guard let value = ProcessInfo.processInfo.environment["AGENT_INTERACTION_PORT"],
        let number = UInt16(value), let port = NWEndpoint.Port(rawValue: number)
      else { return nil }
      return Self(port: port)
    }

    var body: some View {
      MainChatView(model: lifetime.model)
        .task {
          let directory = lifetime.directory
          let model = lifetime.model
          let session = CoordinatorSession(
            outboxURL: directory.appending(path: "pending.json"),
            connectTransport: { _ in
              LineConnection(
                connection: NWConnection(host: .ipv4(.loopback), port: port, using: .tcp))
            })
          defer { try? FileManager.default.removeItem(at: directory) }
          do {
            await model.restoreDraft()
            await model.connect(
              session: session, address: try CoordinatorAddress(host: "100.64.1.2"))
            await model.runUntilCancelled()
          } catch { model.error = error.localizedDescription }
          _ = await model.saveDraft()
        }
    }
  }
#endif
