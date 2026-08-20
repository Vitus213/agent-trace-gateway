//! OTLP/HTTP export of turn records (JSON encoding) to the configured
//! endpoint. Fail-open by design: bounded queue, drop on overflow or endpoint
//! failure, health counters observable — business traffic is never blocked.
use crate::trace::store::TurnRecord;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const QUEUE_CAPACITY: usize = 1024;
const BATCH_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Default)]
pub struct ExportHealth {
    pub exported: std::sync::atomic::AtomicU64,
    pub failed: std::sync::atomic::AtomicU64,
    pub dropped: std::sync::atomic::AtomicU64,
}

pub struct Exporter {
    tx: Option<mpsc::Sender<TurnRecord>>,
    pub health: Arc<ExportHealth>,
}

impl Exporter {
    /// Create an exporter toward `endpoint` (e.g.
    /// http://host/api/public/otel). Returns a disabled exporter (tx=None)
    /// when no endpoint is configured or no tokio runtime is available.
    pub fn start(endpoint: Option<String>) -> Self {
        let health = Arc::new(ExportHealth::default());
        let Some(endpoint) = endpoint.filter(|s| !s.trim().is_empty()) else {
            return Self { tx: None, health };
        };
        let (tx, rx) = mpsc::channel::<TurnRecord>(QUEUE_CAPACITY);
        let health2 = health.clone();
        // The gateway proxy runs on pingora's threads (no ambient tokio
        // runtime), so the exporter owns a dedicated current-thread runtime.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("export runtime");
            rt.block_on(export_loop(endpoint, rx, health2));
        });
        Self { tx: Some(tx), health }
    }

    /// Queue one record for export. Never blocks; drops (counted) when the
    /// queue is full.
    pub fn submit(&self, record: &TurnRecord) {
        let Some(tx) = &self.tx else { return };
        match tx.try_send(record.clone()) {
            Ok(()) => {}
            Err(_) => {
                self.health
                    .dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

async fn export_loop(endpoint: String, mut rx: mpsc::Receiver<TurnRecord>, health: Arc<ExportHealth>) {
    let client = reqwest_client();
    let mut buf: Vec<TurnRecord> = Vec::new();
    let mut last_flush = Instant::now();
    loop {
        match tokio::time::timeout(BATCH_INTERVAL, rx.recv()).await {
            Ok(Some(rec)) => buf.push(rec),
            Ok(None) => break, // channel closed
            Err(_) => {}       // tick: flush if anything buffered
        }
        if buf.is_empty() || last_flush.elapsed() < BATCH_INTERVAL && buf.len() < 32 {
            continue;
        }
        let batch = std::mem::take(&mut buf);
        last_flush = Instant::now();
        flush_batch(&client, &endpoint, &batch, &health).await;
    }
    if !buf.is_empty() {
        flush_batch(&client, &endpoint, &buf, &health).await;
    }
}

fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default()
}

async fn flush_batch(
    client: &reqwest::Client,
    endpoint: &str,
    batch: &[TurnRecord],
    health: &ExportHealth,
) {
    let payload = build_otlp_json(batch);
    let result = client
        .post(endpoint)
        .header("content-type", "application/json")
        .body(payload)
        .send()
        .await;
    match result {
        Ok(resp) if resp.status().is_success() => {
            health
                .exported
                .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        _ => {
            health
                .failed
                .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// Minimal OTLP/HTTP JSON: one resourceSpans with a scopeSpans holding one
/// span per turn (session -> turn organization is expressed through the
/// session.id attribute; consumers group by it).
fn build_otlp_json(batch: &[TurnRecord]) -> String {
    let spans: Vec<serde_json::Value> = batch
        .iter()
        .map(|r| {
            serde_json::json!({
                "traceId": trace_id_for(&r.session_id),
                "spanId": span_id_for(&r.session_id, &r.user_input, &r.raw_request),
                "name": "agent.turn",
                "kind": 3,
                "startTimeUnixNano": "0",
                "endTimeUnixNano": "0",
                "attributes": [
                    kv("session.id", &r.session_id),
                    kv("protocol", &r.protocol),
                    kv("user_input", &r.user_input),
                    kv("final_output", &r.final_output),
                    kv("raw_request", &r.raw_request),
                    kv("raw_response", &r.raw_response),
                    kv("breakpoint", if r.breakpoint { "true" } else { "false" }),
                ]
            })
        })
        .collect();
    serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [kv("service.name", "agent-trace-gateway")]
            },
            "scopeSpans": [{
                "scope": {"name": "agent-trace-gateway"},
                "spans": spans
            }]
        }]
    })
    .to_string()
}

fn kv(key: &str, value: &str) -> serde_json::Value {
    serde_json::json!({"key": key, "value": {"stringValue": value}})
}

fn trace_id_for(session_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"trace:");
    h.update(session_id.as_bytes());
    hex::encode(&h.finalize()[..16])
}

fn span_id_for(session_id: &str, user_input: &str, raw_request: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"span:");
    h.update(session_id.as_bytes());
    h.update(user_input.as_bytes());
    h.update(raw_request.len().to_be_bytes());
    hex::encode(&h.finalize()[..8])
}

/// Test helper: current health counters.
impl ExportHealth {
    pub fn snapshot(&self) -> (u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.exported.load(Relaxed),
            self.failed.load(Relaxed),
            self.dropped.load(Relaxed),
        )
    }
}

#[allow(dead_code)]
fn _unused(_: &Mutex<()>) {}
