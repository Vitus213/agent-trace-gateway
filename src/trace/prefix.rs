//! Prefix stitching state: fingerprint chains keyed by (credential scope,
//! head = system+first-user). State is O(turns) — one 32-byte fingerprint per
//! message, never the history bytes — so very large conversations do not
//! amplify memory.
//!
//! Rules (empirically calibrated against real omp traffic):
//! - chain ⊆ new messages (strict prefix)  -> extend, same synthetic session
//! - same head but chain diverges/shortens -> new segment + breakpoint mark
//! - new head                              -> new session, no mark (a different
//!   conversation cannot be told from a rewrite, so nothing is asserted)
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

const FINGERPRINT_KIND: &str = "pfx";

pub struct PrefixStitcher {
    states: Mutex<HashMap<(String, String), ChainState>>,
}

struct ChainState {
    /// Per-message fingerprints of the chain head, in order.
    chain: Vec<[u8; 32]>,
    /// Synthetic session id of the current segment.
    session_id: String,
}

impl Default for PrefixStitcher {
    fn default() -> Self {
        Self::new()
    }
}

impl PrefixStitcher {
    pub fn new() -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
        }
    }

    /// Classify one credential-scoped request into a synthetic session.
    /// Returns (session_id, breakpoint).
    pub fn assign(&self, scope: &str, messages: &[serde_json::Value]) -> (String, bool) {
        let head = head_key(messages);
        let fps: Vec<[u8; 32]> = messages.iter().map(message_fingerprint).collect();
        let mut states = self.states.lock();
        let entry = states.entry((scope.to_string(), head)).or_insert_with(|| {
            // First sighting of this head: open a fresh chain.
            ChainState {
                session_id: new_session_id(&fps_concat(&fps)),
                chain: fps.clone(),
            }
        });
        if entry.chain.is_empty() {
            entry.chain = fps.clone();
            entry.session_id = new_session_id(&fps_concat(&fps));
            return (entry.session_id.clone(), false);
        }
        if fps.len() >= entry.chain.len() && fps[..entry.chain.len()] == entry.chain[..] {
            // Strict prefix extension.
            entry.chain = fps;
            return (entry.session_id.clone(), false);
        }
        // Same head, divergent or shortened history: compaction breakpoint.
        let new_session = new_session_id(&fps_concat(&fps));
        entry.chain = fps;
        entry.session_id = new_session.clone();
        (new_session, true)
    }
}

/// Head key: fingerprints of the first two messages (system + first user).
fn head_key(messages: &[serde_json::Value]) -> String {
    let mut h = Sha256::new();
    for m in messages.iter().take(2) {
        h.update(message_fingerprint(m));
    }
    hex::encode(h.finalize())
}

fn message_fingerprint(m: &serde_json::Value) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(m.to_string().as_bytes());
    h.finalize().into()
}

fn fps_concat(fps: &[[u8; 32]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(fps.len() * 32);
    for f in fps {
        out.extend_from_slice(f);
    }
    out
}

fn new_session_id(seed: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(FINGERPRINT_KIND.as_bytes());
    h.update(seed);
    let hexed = hex::encode(h.finalize());
    format!("{FINGERPRINT_KIND}:{}", &hexed[..32])
}
