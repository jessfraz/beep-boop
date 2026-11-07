import Foundation

extension SourceKey: Hashable {
  public func hash(into hasher: inout Hasher) {
    hasher.combine(provider)
    hasher.combine(account)
  }
}

extension SourceState {
  public var displayName: String {
    switch source.provider {
    case "google": "Gmail"
    case "google_calendar": "Calendar"
    case "github": "GitHub"
    case "slack": "Slack"
    case "messages": "Messages"
    case "task_outcomes": "Task outcomes"
    default: "Other source"
    }
  }
  public var lastChecked: Date? {
    checkedAt > 0 ? Date(timeIntervalSince1970: Double(checkedAt) / 1000) : nil
  }
  public var lastCompleted: Date? {
    lastCompletedAt.map { Date(timeIntervalSince1970: Double($0) / 1000) }
  }
  public var statusLabel: String {
    if case .more = coverage {
      return lastCompletedAt == nil ? "Catching up" : "Refreshing"
    }
    return coverage.label
  }
}

extension SourceScanProgress {
  public var summary: String {
    let noun: String =
      switch unit {
      case .calendars: "calendars"
      case .chats: "chats"
      case .messages: "messages"
      }
    let action = phase == .discovering ? "Discovering" : "Reading"
    switch (completed, total) {
    case (.some(let completed), .some(let total)):
      let qualifier = unit == .messages ? "about " : ""
      return "\(action) \(noun): \(completed) of \(qualifier)\(total)"
    case (.some(let completed), .none):
      return "\(action) \(noun): \(completed)"
    case (.none, .some(let total)):
      return "\(action) \(noun): about \(total) matches"
    case (.none, .none): return "\(action) \(noun)"
    }
  }
  public var scanStart: Date { Date(timeIntervalSince1970: Double(windowStart)) }
  public var scanEnd: Date { Date(timeIntervalSince1970: Double(windowEnd)) }
}

extension SourceCoverage {
  public var label: String {
    switch self {
    case .complete: "Current"
    case .more: "Catching up"
    case .partial: "Partial"
    case .unavailable: "Unavailable"
    }
  }
  /// The source adapter supplies this safe diagnostic, excluding helper payloads.
  public var diagnostic: String? {
    switch self {
    case .partial(let value): value.message
    case .unavailable(let value): value.message
    case .complete, .more: nil
    }
  }
}
