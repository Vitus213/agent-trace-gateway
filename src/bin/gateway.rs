//! agent-trace-gateway entrypoint.
//! Env: ATG_LISTEN (default 127.0.0.1:6180), ATG_UPSTREAM (required),
//! ATG_OTLP_ENDPOINT (optional), ATG_CAPTURE_MAX_BYTES, ATG_STITCH_CAPACITY,
//! ATG_STITCH_TTL_MS.
fn main() {
    let listen = std::env::var("ATG_LISTEN").unwrap_or_else(|_| "127.0.0.1:6180".to_string());
    let upstream = match std::env::var("ATG_UPSTREAM") {
        Ok(u) if !u.trim().is_empty() => u,
        _ => {
            eprintln!("agent-trace-gateway: ATG_UPSTREAM is required (host:port)");
            std::process::exit(2);
        }
    };
    agent_trace_gateway::gateway_app::run(&listen, &upstream);
}
