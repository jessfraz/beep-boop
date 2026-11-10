use crate::{
    native::{self, NativeEvent},
    protocol::ToolStatus,
    tui::{HistoryLoad, MessageKind, TuiInput},
};

#[path = "../../../tests/fixtures/tool_merge.rs"]
mod contract;

#[test]
fn shared_tool_merge_traces_keep_body_status_identity_and_sparse_metadata() {
    let traces: contract::Traces = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/ios/Tests/AgentCoreTests/Fixtures/tool-merge-traces.json"
    )))
    .unwrap();
    assert_eq!(traces.version, 1);
    for case in traces.cases {
        let mut view = super::state();
        for step in case.steps {
            // Both clients consume the real native wire for every trace step.
            let event = native::project(&step.event).unwrap();
            let wire = serde_json::to_vec(&event).unwrap();
            let _: NativeEvent = serde_json::from_slice(&wire).unwrap();
            if step.older_page {
                view.more_history = true;
                assert_eq!(
                    view.load_history(HistoryLoad::Older),
                    Some(TuiInput::History)
                );
            }
            view.event(step.event);
        }
        let tools = view
            .messages
            .iter()
            .filter(|message| matches!(message.kind, MessageKind::Tool { .. }))
            .collect::<Vec<_>>();
        assert_eq!(tools.len(), 1, "{}", case.name);
        let row = tools[0];
        assert_eq!(
            row.body_entry.as_ref().or(row.id.as_ref()),
            Some(&case.expected.entry_id),
            "{}",
            case.name
        );
        assert_eq!(row.text, case.expected.text, "{}", case.name);
        let MessageKind::Tool { call_id, status } = &row.kind else {
            unreachable!()
        };
        assert_eq!(call_id, &case.expected.tool.call_id, "{}", case.name);
        assert_eq!(status, &case.expected.tool.status, "{}", case.name);
        assert_eq!(row.tool_name, case.expected.tool.name, "{}", case.name);
        assert_eq!(
            serde_json::to_string(&row.presentation).unwrap(),
            serde_json::to_string(&case.expected.presentation).unwrap(),
            "{}",
            case.name
        );
        assert!(
            !matches!(status, ToolStatus::Running | ToolStatus::Unknown),
            "{}",
            case.name
        );
    }
}
