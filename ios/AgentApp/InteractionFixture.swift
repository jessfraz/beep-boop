#if DEBUG
  import Foundation
  import Network
  import SwiftUI

  @testable import AgentCore
  @testable import AgentUI

  // UI automation redirects only transport. The screen owns production startup,
  // persistence and foreground recovery; release builds have no override.
  struct InteractionFixture: View {
    @MainActor private struct Lifetime {
      let model: ChatModel
      let defaults: UserDefaults

      init?(port: NWEndpoint.Port, storageID: UUID) {
        let key = storageID.uuidString.lowercased()
        guard let defaults = UserDefaults(suiteName: "dev.jessfraz.Agent.Interaction.\(key)") else {
          return nil
        }
        self.defaults = defaults
        defaults.register(defaults: [
          "coordinatorAddress": "session-\(key).fixture.ts.net",
          "coordinatorPort": String(port.rawValue),
        ])
        let directory = URL.applicationSupportDirectory.appending(path: "Agent/Interaction/\(key)")
        model = ChatModel.persistent(
          in: directory,
          makeSession: { outbox, conversation in
            CoordinatorSession(
              outboxURL: outbox, inspecting: conversation,
              connectTransport: { _ in
                LineConnection(
                  connection: NWConnection(host: .ipv4(.loopback), port: port, using: .tcp))
              })
          })
      }
    }

    // Reconstruction must keep the model and its settings/store identity together.
    @State private var lifetime: Lifetime
    private let notifications: AppNotifications

    private init(lifetime: Lifetime, notifications: AppNotifications) {
      _lifetime = State(initialValue: lifetime)
      self.notifications = notifications
    }

    static func fromEnvironment(notifications: AppNotifications) -> Self? {
      guard let value = ProcessInfo.processInfo.environment["AGENT_INTERACTION_PORT"],
        let number = UInt16(value), let port = NWEndpoint.Port(rawValue: number)
      else { return nil }
      let storageID =
        ProcessInfo.processInfo.environment["AGENT_INTERACTION_ID"]
        .flatMap(UUID.init(uuidString:)) ?? UUID()
      guard let lifetime = Lifetime(port: port, storageID: storageID) else { return nil }
      return Self(lifetime: lifetime, notifications: notifications)
    }

    var body: some View {
      MainChatView(model: lifetime.model, managesConnection: true, notifications: notifications)
        .defaultAppStorage(lifetime.defaults)
    }
  }
#endif
