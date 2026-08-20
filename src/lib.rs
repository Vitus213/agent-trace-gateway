// agent-trace-gateway library: harness modules + gateway app.
pub mod harness;
pub mod trace;

pub mod gateway_app {
    use async_trait::async_trait;
    use bytes::Bytes;
    use pingora::http::ResponseHeader;
    use pingora::prelude::*;
    use pingora::proxy::{http_proxy_service, ProxyHttp, Session};
    use pingora::upstreams::peer::HttpPeer;

    use crate::trace::store::TraceStore;
    use crate::trace::unpack;
    use crate::trace::session;

    pub struct Gateway {
        pub upstream: String,
        pub store: TraceStore,
        pub stitcher: crate::trace::prefix::PrefixStitcher,
    }

    pub struct Ctx {
        pub req_buf: Vec<u8>,
        pub resp_buf: Vec<u8>,
        pub resp_content_type: String,
        pub ws_client_parser: crate::trace::ws::WsFrameParser,
        pub ws_server_parser: crate::trace::ws::WsFrameParser,
        pub ws_turn: crate::trace::ws::WsTurnState,
    }

    #[async_trait]
    impl ProxyHttp for Gateway {
        type CTX = Ctx;

        fn new_ctx(&self) -> Self::CTX {
            Ctx {
                req_buf: Vec::new(),
                resp_buf: Vec::new(),
                resp_content_type: String::new(),
                ws_client_parser: crate::trace::ws::WsFrameParser::new(true),
                ws_server_parser: crate::trace::ws::WsFrameParser::new(false),
                ws_turn: crate::trace::ws::WsTurnState::default(),
            }
        }

        async fn upstream_peer(
            &self,
            _session: &mut Session,
            _ctx: &mut Self::CTX,
        ) -> Result<Box<HttpPeer>> {
            Ok(Box::new(HttpPeer::new(
                self.upstream.clone(),
                false,
                String::new(),
            )))
        }

        // Control endpoint: dump collected turn records as JSON.
        async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool> {
            if session.req_header().uri.path() == "/__atg/records" {
                let records = self.store.snapshot();
                let body = serde_json::to_vec(&records).unwrap_or_default();
                let mut resp = ResponseHeader::build(200, None)?;
                resp.insert_header("content-type", "application/json")?;
                resp.insert_header("content-length", body.len().to_string())?;
                session.write_response_header(Box::new(resp), false).await?;
                session.write_response_body(Some(Bytes::from(body)), true).await?;
                return Ok(true);
            }
            Ok(false)
        }

        async fn request_body_filter(
            &self,
            session: &mut Session,
            body: &mut Option<Bytes>,
            _end: bool,
            ctx: &mut Self::CTX,
        ) -> Result<()> {
            if session.was_upgraded() {
                if let Some(b) = body {
                    for payload in ctx.ws_client_parser.push(b) {
                        ctx.ws_turn.apply_client_frame(&payload);
                    }
                }
            } else if let Some(b) = body {
                ctx.req_buf.extend_from_slice(b);
            }
            Ok(())
        }

        async fn upstream_response_filter(
            &self,
            _session: &mut Session,
            resp: &mut pingora::http::ResponseHeader,
            ctx: &mut Self::CTX,
        ) -> Result<()> {
            if let Some(v) = resp.headers.get(http::header::CONTENT_TYPE) {
                ctx.resp_content_type = v.to_str().unwrap_or("").to_string();
            }
            Ok(())
        }

        fn response_body_filter(
            &self,
            session: &mut Session,
            body: &mut Option<Bytes>,
            _end: bool,
            ctx: &mut Self::CTX,
        ) -> Result<Option<std::time::Duration>> {
            if session.was_upgraded() {
                if let Some(b) = body {
                    for payload in ctx.ws_server_parser.push(b) {
                        if let Some(record) = ctx.ws_turn.apply_server_frame(&payload) {
                            self.store.push(record);
                        }
                    }
                }
            } else if let Some(b) = body {
                ctx.resp_buf.extend_from_slice(b);
            }
            Ok(None)
        }

        async fn logging(&self, session: &mut Session, _e: Option<&Error>, ctx: &mut Self::CTX) {
            let path = session.req_header().uri.path();
            let Some(protocol) = unpack::detect_protocol(path) else {
                return;
            };
            let header_get = |name: &str| -> Option<String> {
                session
                    .req_header()
                    .headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            };
            let mut session_id =
                session::extract_session_id(protocol, &ctx.req_buf, &header_get).unwrap_or_default();
            let mut breakpoint = false;
            if session_id.is_empty() {
                if let Some(messages) = unpack::extract_messages(&ctx.req_buf) {
                    let scope = header_get("authorization").unwrap_or_default();
                    let (synthetic, is_bp) = self.stitcher.assign(&scope, &messages);
                    session_id = synthetic;
                    breakpoint = is_bp;
                }
            }
            if unpack::looks_like_sse(&ctx.resp_content_type) {
                let final_output = unpack::reassemble_sse_output(protocol, &ctx.resp_buf);
                let user_input =
                    unpack::extract_user_input(protocol, &ctx.req_buf).unwrap_or_default();
                self.store.push(crate::trace::store::TurnRecord {
                    protocol: protocol.to_string(),
                    session_id,
                    user_input,
                    final_output,
                    breakpoint,
                    ..Default::default()
                });
                return;
            }
            if let Some(mut record) =
                unpack::unpack_nonstreaming(protocol, &ctx.req_buf, &ctx.resp_buf)
            {
                record.session_id = session_id;
                record.breakpoint = breakpoint;
                self.store.push(record);
            }
        }
    }

    /// Start the gateway on `listen`, forwarding to `upstream`. Blocks.
    pub fn run(listen: &str, upstream: &str) {
        let mut server = Server::new(Some(Opt::default())).unwrap();
        server.bootstrap();
        let gateway = Gateway {
            upstream: upstream.to_string(),
            store: TraceStore::new(),
            stitcher: crate::trace::prefix::PrefixStitcher::new(),
        };
        let mut http_proxy = http_proxy(&server.configuration, gateway);
        let mut opts = pingora::apps::HttpServerOptions::default();
        opts.h2c = true;
        http_proxy.server_options = Some(opts);
        let mut svc = pingora::services::listening::Service::new("agent-trace-gateway".to_string(), http_proxy);
        svc.add_tcp(listen);
        server.add_service(svc);
        server.run_forever();
    }
}
