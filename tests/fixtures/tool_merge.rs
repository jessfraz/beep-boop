use crate::protocol::{MessagePresentation, RuntimeEventKind, ToolHistory};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Traces {
    pub version: u32,
    pub cases: Vec<Case>,
}

#[derive(Deserialize)]
pub struct Case {
    pub name: String,
    pub steps: Vec<Step>,
    pub expected: Expected,
}

#[derive(Deserialize)]
pub struct Step {
    pub older_page: bool,
    pub event: RuntimeEventKind,
}

#[derive(Deserialize)]
pub struct Expected {
    pub entry_id: String,
    pub text: String,
    pub tool: ToolHistory,
    pub presentation: Option<Box<MessagePresentation>>,
}
