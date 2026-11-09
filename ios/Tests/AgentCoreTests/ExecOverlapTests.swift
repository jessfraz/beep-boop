#if os(macOS)
  import Foundation
  import Testing

  @MainActor @Test(.timeLimit(.minutes(3)))
  func realCommandPersistenceDuringFoundationProcessExec() async throws {
    try await withThrowingTaskGroup(of: Void.self) { group in
      group.addTask {
        for _ in 0..<500 {
          let process = Process()
          process.executableURL = URL(filePath: "/usr/bin/true")
          try process.run()
          process.waitUntilExit()
          #expect(process.terminationStatus == 0)
        }
      }
      for _ in 0..<32 {
        group.addTask {
          for _ in 0..<64 {
            try await commandInsertionPreservesTheDurableNoteAndReplacesOnlyKnownCommands(
              draft: "/help Keep the note.", expected: "/feedback Keep the note.")
          }
        }
      }
      try await group.waitForAll()
    }
  }
#endif
