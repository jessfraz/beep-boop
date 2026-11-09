import Foundation
import OSLog

/// Only the composer's metadata logger is eligible for a feedback attachment.
enum ComposerDiagnostics {
  enum Coverage: String, Sendable { case available, empty, unavailable }
  struct Export: Sendable {
    let coverage: Coverage
    let recordCount: Int
    let truncated: Bool
    let text: String
  }

  static func export() async -> Export {
    let results = AsyncStream<Export>.makeStream(bufferingPolicy: .bufferingNewest(1))
    // A synchronous OSLogStore read cannot hold feedback past its deadline.
    // Cancel its remaining enumeration without waiting for the system call.
    let reader = Task.detached(priority: .utility) {
      results.continuation.yield(read())
      results.continuation.finish()
    }
    let deadline = Task {
      do { try await Task.sleep(for: .seconds(2)) } catch { return }
      results.continuation.yield(unavailable("time limit"))
      results.continuation.finish()
    }
    defer {
      reader.cancel()
      deadline.cancel()
      results.continuation.finish()
    }
    for await result in results.stream { return result }
    return unavailable("cancelled")
  }

  private static func read() -> Export {
    let subsystem = "dev.jessfraz.Agent"
    let category = "Composer"
    let now = Date()
    let since = now.addingTimeInterval(-15 * 60)
    let maximumBytes = 64 * 1024
    // The fixed coverage header remains inside the attachment's byte budget.
    let headerBudget = 512
    do {
      let store = try OSLogStore(scope: .currentProcessIdentifier)
      let entries = try store.getEntries(
        at: store.position(date: since),
        matching: NSPredicate(format: "subsystem == %@ AND category == %@", subsystem, category))
      var lines: [String] = []
      var bytes = 0
      var truncated = false
      for entry in entries {
        guard !Task.isCancelled else { return unavailable("cancelled") }
        // Current-process stores can ignore the starting position and reverse
        // order. Keep only this export's bounded selection of recent records.
        guard entry.date >= since, entry.date <= now, let log = entry as? OSLogEntryLog,
          log.subsystem == subsystem, log.category == category
        else { continue }
        let line = "\(log.date.ISO8601Format()) \(log.composedMessage)\n"
        guard line.utf8.count <= maximumBytes - headerBudget else {
          truncated = true
          continue
        }
        lines.append(line)
        bytes += line.utf8.count
        while lines.count > 256 || bytes > maximumBytes - headerBudget {
          bytes -= lines.removeFirst().utf8.count
          truncated = true
        }
      }
      let coverage: Coverage = lines.isEmpty ? .empty : .available
      let header =
        "Composer diagnostics: coverage=\(coverage.rawValue) records=\(lines.count) truncated=\(truncated)\n"
        + "scope=current-process window=15m order=oldest-first subsystem=\(subsystem) category=\(category)\n"
      return Export(
        coverage: coverage, recordCount: lines.count, truncated: truncated,
        text: header + lines.joined())
    } catch {
      return unavailable("current-process log access")
    }
  }

  private static func unavailable(_ reason: String) -> Export {
    Export(
      coverage: .unavailable, recordCount: 0, truncated: false,
      text: "Composer diagnostics: coverage=unavailable reason=\(reason)\n")
  }
}
