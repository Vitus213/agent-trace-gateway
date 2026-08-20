//! In-process turn record store (observable via the control endpoint).
use parking_lot::Mutex;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Default)]
pub struct TurnRecord {
    pub protocol: String,
    pub session_id: String,
    pub user_input: String,
    pub final_output: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tool_calls: Vec<ToolCall>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolCall {
    pub name: String,
    pub arguments: String,
}

#[derive(Clone, Default)]
pub struct TraceStore {
    records: Arc<Mutex<Vec<TurnRecord>>>,
}

impl TraceStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, record: TurnRecord) {
        self.records.lock().push(record);
    }

    pub fn snapshot(&self) -> Vec<TurnRecord> {
        self.records.lock().clone()
    }
}
