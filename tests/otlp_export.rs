// Behavior: completed turns are exported as OTLP traces to the configured
// endpoint, organized as session -> turn spans.
// [Requirement: 轨迹导出与故障恢复；Scenario: 轨迹到达审计系统]
mod common;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use hyper_util::server::conn::auto::Builder;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::net::TcpListener;

fn client() -> Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>> {
    Client::builder(TokioExecutor::new()).build_http()
}

async fn fake_collector(port: u16, received: Arc<Mutex<Vec<Vec<u8>>>>) {
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await.unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let received = received.clone();
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let svc = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                    let received = received.clone();
                    async move {
                        let body = req.collect().await.unwrap().to_bytes();
                        received.lock().push(body.to_vec());
                        Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(Bytes::from("{}"))))
                    }
                });
                let _ = Builder::new(TokioExecutor::new()).serve_connection(io, svc).await;
            });
        }
    });
}

async fn records(gw: u16) -> Vec<serde_json::Value> {
    let req = Request::get(format!("http://127.0.0.1:{gw}/__atg/records"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client().request(req).await.expect("records");
    serde_json::from_slice(&resp.collect().await.unwrap().to_bytes()).expect("records JSON")
}

#[tokio::test]
async fn otlp_export() {
    let collector_port = common::stack::fixture_port() + 100;
    let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    fake_collector(collector_port, received.clone()).await;

    common::stack::start_stack_with_env(&[(
        "ATG_OTLP_ENDPOINT",
        format!("http://127.0.0.1:{collector_port}/api/public/otel"),
    )])
    .await;
    let gw = common::stack::gateway_port();

    // One explicit-session turn through the gateway.
    let body = serde_json::json!({
        "model": "m",
        "messages": [{"role": "user", "content": "otlp-turn"}]
    });
    let req = Request::post(format!("http://127.0.0.1:{gw}/v1/chat"))
        .header("content-type", "application/json")
        .header("x-claude-code-session-id", "otlp-session-1")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let resp = client().request(req).await.expect("request");
    assert_eq!(resp.status(), StatusCode::OK);
    let _ = resp.collect().await.unwrap();

    // Wait for the export flush.
    let mut exported = Vec::new();
    for _ in 0..50 {
        exported = received.lock().clone();
        if !exported.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(!exported.is_empty(), "collector received nothing");

    // Parse the OTLP JSON payload: session -> turn span structure.
    let payload: serde_json::Value =
        serde_json::from_slice(&exported[0]).expect("OTLP payload must be JSON");
    let spans = payload["resourceSpans"]
        .as_array()
        .and_then(|rs| rs.first())
        .and_then(|rs| rs["scopeSpans"].as_array())
        .and_then(|ss| ss.first())
        .and_then(|s| s["spans"].as_array())
        .unwrap_or_else(|| panic!("OTLP spans missing: {payload}"));
    assert_eq!(spans.len(), 1, "one turn span expected: {spans:?}");
    let span = &spans[0];
    // Session id present as an attribute; turn content carried verbatim.
    let attrs = span["attributes"].as_array().expect("span attributes");
    let attr = |k: &str| {
        attrs
            .iter()
            .find(|a| a["key"] == k)
            .and_then(|a| a["value"]["stringValue"].as_str())
            .map(str::to_string)
            .unwrap_or_default()
    };
    assert_eq!(attr("session.id"), "otlp-session-1", "session attribute: {attrs:?}");
    assert_eq!(attr("protocol"), "openai_chat");
    assert!(
        attr("user_input").contains("otlp-turn"),
        "user input must be exported: {attrs:?}"
    );
    assert!(
        attr("raw_request").contains("otlp-turn"),
        "verbatim request must be exported: {attrs:?}"
    );

    // The record store still holds the record (export does not mutate it).
    let recs = records(gw).await;
    assert_eq!(recs.len(), 1);
}
