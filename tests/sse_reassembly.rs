// Behavior: SSE streaming responses are reassembled into the final output;
// reassembled result equals the concatenation of streamed deltas.
// [Requirement: 协议解包与流式重组；Scenario: SSE 流式响应重组]
mod common;

use bytes::Bytes;
use common::stack::start_stack;
use common::stack::GATEWAY_PORT;
use futures_util::StreamExt;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

fn client() -> Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>> {
    Client::builder(TokioExecutor::new()).build_http()
}

#[tokio::test]
async fn sse_reassembly() {
    start_stack().await;

    // Streaming Responses request: fixture emits two output_text deltas + completed.
    let req = Request::post(format!("http://127.0.0.1:{GATEWAY_PORT}/v1/responses"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            r#"{"model":"m","stream":true,"input":"sse-user-text"}"#,
        )))
        .unwrap();
    let resp = client().request(req).await.expect("stream request");
    assert_eq!(resp.status(), 200);
    let mut stream = resp.into_data_stream();
    let mut raw = Vec::new();
    while let Some(chunk) = stream.next().await {
        raw.extend_from_slice(&chunk.expect("chunk"));
    }
    let raw_text = String::from_utf8_lossy(&raw).to_string();
    assert!(raw_text.contains("response.completed"), "stream incomplete");

    // Give the gateway logging phase a moment to record the turn.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let req = Request::get(format!("http://127.0.0.1:{GATEWAY_PORT}/__atg/records"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client().request(req).await.expect("records");
    let recs: serde_json::Value =
        serde_json::from_slice(&resp.collect().await.unwrap().to_bytes()).expect("records JSON");
    let arr = recs.as_array().expect("records array");
    let rec = arr
        .iter()
        .find(|r| r["protocol"] == "openai_responses")
        .unwrap_or_else(|| panic!("no openai_responses record in {arr:?}"));

    assert_eq!(
        rec["user_input"], "sse-user-text",
        "streaming turn user input: {rec}"
    );
    // Deltas "echo-stream:" + "part2" must be concatenated in order.
    assert_eq!(
        rec["final_output"], "echo-stream:part2",
        "SSE deltas must reassemble to the concatenated text: {rec}"
    );
}
