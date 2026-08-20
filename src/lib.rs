// agent-trace-gateway library: harness modules + gateway app.
pub mod harness;

pub mod gateway_app {
    use async_trait::async_trait;
    use pingora::prelude::*;
    use pingora::proxy::{http_proxy_service, ProxyHttp, Session};
    use pingora::upstreams::peer::HttpPeer;

    pub struct Gateway {
        pub upstream: String,
    }

    #[async_trait]
    impl ProxyHttp for Gateway {
        type CTX = ();

        fn new_ctx(&self) -> Self::CTX {}

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

    }

    /// Start the gateway on `listen`, forwarding to `upstream`. Blocks.
    pub fn run(listen: &str, upstream: &str) {
        let mut server = Server::new(Some(Opt::default())).unwrap();
        server.bootstrap();
        let gateway = Gateway {
            upstream: upstream.to_string(),
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
