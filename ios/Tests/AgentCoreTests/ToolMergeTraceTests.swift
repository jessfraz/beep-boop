import Foundation
import Testing

@testable import AgentCore

// Rust and Swift consume the same producer-validated ordered traces. These
// fixture envelopes are test data; all events and expectations use shared DTOs.
private struct ToolMergeTraces: Decodable {
  let version: Int
  let cases: [ToolMergeTrace]
}

private struct ToolMergeTrace: Decodable {
  struct Step: Decodable {
    let olderPage: Bool
    let event: NativeEvent
    enum CodingKeys: String, CodingKey {
      case olderPage = "older_page"
      case event
    }
  }
  struct Expected: Decodable {
    let entryID: String
    let text: String
    let tool: ToolHistory
    let presentation: MessagePresentation?
    enum CodingKeys: String, CodingKey {
      case entryID = "entry_id"
      case text, tool, presentation
    }
  }
  let name: String
  let steps: [Step]
  let expected: Expected
}

@Test func sharedToolMergeTracesPreserveNativeIdentityBodyStatusAndMetadata() throws {
  let url = try #require(
    Bundle.module.url(
      forResource: "tool-merge-traces", withExtension: "json", subdirectory: "Fixtures"))
  let traces = try JSONDecoder().decode(ToolMergeTraces.self, from: Data(contentsOf: url))
  #expect(traces.version == 1)
  try #require(!traces.cases.isEmpty)
  for trace in traces.cases {
    var transcript = Transcript()
    for step in trace.steps {
      switch step.event {
      case .message(let value): transcript.message(value.message)
      case .history(let value): transcript.history(value, olderPage: step.olderPage)
      case .activity(let value): transcript.activity(value.activity)
      default: Issue.record("Unexpected tool merge event in \(trace.name)")
      }
    }
    let row = try #require(transcript.rows.first, "Missing merged row in \(trace.name)")
    #expect(transcript.rows.count == 1, "Duplicate tool rows in \(trace.name)")
    #expect(row.entryID == trace.expected.entryID, "Changed identity in \(trace.name)")
    #expect(row.text == trace.expected.text, "Changed body in \(trace.name)")
    #expect(row.tool == trace.expected.tool, "Changed tool state in \(trace.name)")
    #expect(row.presentation == trace.expected.presentation, "Lost metadata in \(trace.name)")
  }
}
