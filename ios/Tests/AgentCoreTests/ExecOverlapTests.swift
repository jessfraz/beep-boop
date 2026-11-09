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
          // Process waits must yield the cooperative executor so the real
          // store and transport actors running beside this load can progress.
          try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<Void, any Error>) in
            process.terminationHandler = { _ in continuation.resume() }
            do { try process.run() } catch {
              process.terminationHandler = nil
              continuation.resume(throwing: error)
            }
          }
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
