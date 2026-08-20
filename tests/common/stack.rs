use std::time::Duration;

pub const FIXTURE_PORT: u16 = 17861;
pub const GATEWAY_PORT: u16 = 17860;

/// Start the fixture upstream as a tokio task and the pingora gateway in a
/// background thread (both in-process: endpoint security kills freshly
/// compiled listener child processes, so tests embed the servers).
pub async fn start_stack() {
    let fixture_listen = format!("127.0.0.1:{FIXTURE_PORT}");
    tokio::spawn(async move {
        let _ = agent_trace_gateway::harness::fixture_server::serve(&fixture_listen).await;
    });
    let gw_listen = format!("127.0.0.1:{GATEWAY_PORT}");
    let upstream = format!("127.0.0.1:{FIXTURE_PORT}");
    std::thread::spawn(move || {
        agent_trace_gateway::gateway_app::run(&gw_listen, &upstream);
    });
    for port in [FIXTURE_PORT, GATEWAY_PORT] {
        let addr = format!("127.0.0.1:{port}");
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Start the stack with extra gateway env vars (e.g. stitcher capacity/TTL).
pub async fn start_stack_with_env(extra: &[(&str, String)]) {
    let fixture_listen = format!("127.0.0.1:{FIXTURE_PORT}");
    tokio::spawn(async move {
        let _ = agent_trace_gateway::harness::fixture_server::serve(&fixture_listen).await;
    });
    let gw_listen = format!("127.0.0.1:{GATEWAY_PORT}");
    let upstream = format!("127.0.0.1:{FIXTURE_PORT}");
    let extra: Vec<(String, String)> = extra.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
    std::thread::spawn(move || {
        for (k, v) in &extra {
            std::env::set_var(k, v);
        }
        agent_trace_gateway::gateway_app::run(&gw_listen, &upstream);
    });
    for port in [FIXTURE_PORT, GATEWAY_PORT] {
        let addr = format!("127.0.0.1:{port}");
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
