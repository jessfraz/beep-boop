import Foundation
import Testing

@testable import AgentCore

@Test func sourceRefreshRetainsTheLastCompletedScanAndUnknownHistoryStaysUnknown() throws {
  let source = SourceKey(account: "fixture", provider: "google_calendar")
  let legacy = SourceState(checkedAt: 1_800_000_000_000, coverage: .more, source: source)
  let decoded = try JSONDecoder().decode(SourceState.self, from: JSONEncoder().encode(legacy))
  #expect(decoded.lastCompleted == nil)
  #expect(decoded.statusLabel == "Catching up")
  let refreshing = SourceState(
    checkedAt: 1_800_001_000_000, coverage: .more, lastCompletedAt: 1_800_000_000_000,
    progress: .init(
      completed: 8, phase: .reading, total: 9, unit: .calendars,
      windowEnd: 1_802_592_000, windowStart: 1_799_913_600), source: source)
  #expect(refreshing.statusLabel == "Refreshing")
  #expect(refreshing.lastCompleted == Date(timeIntervalSince1970: 1_800_000_000))
  #expect(refreshing.progress?.summary == "Reading calendars: 8 of 9")
  #expect(refreshing.progress?.scanStart == Date(timeIntervalSince1970: 1_799_913_600))
  #expect(refreshing.progress?.scanEnd == Date(timeIntervalSince1970: 1_802_592_000))
  let unavailable = SourceState(
    checkedAt: refreshing.checkedAt, coverage: .unavailable(.init(message: "Provider unavailable")),
    lastCompletedAt: refreshing.lastCompletedAt, progress: refreshing.progress, source: source)
  #expect(unavailable.statusLabel == "Unavailable")
  #expect(unavailable.lastCompleted == refreshing.lastCompleted)
}

@Test func unknownSlackProgressDoesNotInventHowMuchOfTheOldCursorWasRead() {
  let progress = SourceScanProgress(
    completed: nil, phase: .reading, total: 61_613, unit: .messages,
    windowEnd: 1_800_000_000, windowStart: 1_799_568_000)
  #expect(progress.summary == "Reading messages: about 61613 matches")
}
