//! Protocol unpacking: request/response bytes -> turn facts.
//! Slice 2.1 scope: non-streaming user_input + final_output for the three
//! model protocols. SSE/WS reassembly lands in later slices.
use crate::trace::store::{ToolCall, TurnRecord};

pub fn detect_protocol(path: &str) -> Option<&'static str> {
    if path.starts_with("/v1/chat") || path.starts_with("/compatible-mode/v1/chat") {
        Some("openai_chat")
    } else if path.starts_with("/v1/messages") {
        Some("anthropic_messages")
    } else if path.starts_with("/v1/responses") {
        Some("openai_responses")
    } else {
        None
    }
}

/// Extract user input + final output from one non-streaming request/response
/// pair. Returns None when the protocol is unknown or bodies are not JSON.
pub fn unpack_nonstreaming(protocol: &str, request_body: &[u8], response_body: &[u8]) -> Option<TurnRecord> {
    let req: serde_json::Value = serde_json::from_slice(request_body).ok()?;
    let resp: serde_json::Value = serde_json::from_slice(response_body).ok()?;
    match protocol {
        "openai_chat" => {
            let user_input = req["messages"]
                .as_array()?
                .iter()
                .rev()
                .find(|m| m["role"] == "user")
                .and_then(|m| content_text(&m["content"]))?;
            let final_output = resp["choices"]
                .as_array()
                .and_then(|c| c.first())
                .and_then(|c| content_text(&c["message"]["content"]))
                .unwrap_or_default();
            Some(TurnRecord {
                protocol: protocol.to_string(),
                user_input,
                final_output,
                ..Default::default()
            })
        }
        "anthropic_messages" => {
            let user_input = req["messages"]
                .as_array()?
                .iter()
                .rev()
                .find(|m| m["role"] == "user")
                .and_then(|m| content_text(&m["content"]))?;
            let final_output = resp["content"]
                .as_array()
                .and_then(|blocks| {
                    blocks.iter().find_map(|b| {
                        if b["type"] == "text" {
                            b["text"].as_str().map(str::to_string)
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or_default();
            Some(TurnRecord {
                protocol: protocol.to_string(),
                user_input,
                final_output,
                ..Default::default()
            })
        }
        "openai_responses" => {
            let user_input = req["input"].as_str()?.to_string();
            let final_output = resp["output"]
                .as_array()
                .and_then(|items| {
                    items.iter().find_map(|item| {
                        item["content"].as_array().and_then(|blocks| {
                            blocks.iter().find_map(|b| {
                                if b["type"] == "output_text" {
                                    b["text"].as_str().map(str::to_string)
                                } else {
                                    None
                                }
                            })
                        })
                    })
                })
                .unwrap_or_default();
            Some(TurnRecord {
                protocol: protocol.to_string(),
                user_input,
                final_output,
                ..Default::default()
            })
        }
        _ => None,
    }
}

/// Flatten OpenAI/Anthropic content fields: plain string or block arrays.
fn content_text(content: &serde_json::Value) -> Option<String> {
    if let Some(s) = content.as_str() {
        return Some(s.to_string());
    }
    if let Some(blocks) = content.as_array() {
        let mut out = Vec::new();
        for b in blocks {
            if let Some(t) = b["text"].as_str() {
                out.push(t.to_string());
            }
        }
        if !out.is_empty() {
            return Some(out.join("\n"));
        }
    }
    None
}

#[allow(dead_code)]
fn _keep_toolcall_import() -> Option<ToolCall> {
    None
}
