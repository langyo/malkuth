//! Pluggable transport backends for JSON-RPC.
//!
//! Each backend implements [`malkuth_core::Transport`] and yields
//! [`malkuth_core::FramedConn`]-wrapped streams. They all sit on the
//! `futures_io` traits, so they run under tokio, async-std or smol.

#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(feature = "tcp")]
pub use tcp::TcpTransport;

#[cfg(feature = "ws")]
pub mod ws;
#[cfg(feature = "ws")]
pub use ws::WsTransport;

#[cfg(feature = "ipc")]
pub mod ipc;
#[cfg(feature = "ipc")]
pub use ipc::IpcTransport;

/// A transport that dispatches by URL scheme to the built-in backends.
///
/// Enabled backends are tried in order; the first whose scheme prefix matches
/// the address handles it. Schemes:
/// - `tcp://host:port` (or a bare `host:port`) → [`TcpTransport`]
/// - `ws://…` / `wss://…` → [`WsTransport`]  (feature `ws`)
/// - `ipc:/path` or `ipc:name` → [`IpcTransport`]  (feature `ipc`)
///
/// For unrecognised schemes, falls back to TCP.
pub struct MultiTransport;

#[async_trait::async_trait]
impl malkuth_core::Transport for MultiTransport {
    async fn listen(&self, addr: &str) -> std::io::Result<Box<dyn malkuth_core::WireListener>> {
        self.pick(addr).listen(addr).await
    }
    async fn connect(&self, addr: &str) -> std::io::Result<Box<dyn malkuth_core::WireConn>> {
        self.pick(addr).connect(addr).await
    }
    fn name(&self) -> &'static str {
        "multi"
    }
}

impl MultiTransport {
    /// Pick the backend for `addr` by scheme.
    fn pick(&self, addr: &str) -> Box<dyn malkuth_core::Transport> {
        #[cfg(feature = "ws")]
        if let Some(rest) = addr.strip_prefix("ws://").or_else(|| addr.strip_prefix("wss://")) {
            let _ = rest;
            return Box::new(WsTransport);
        }
        #[cfg(feature = "ipc")]
        if addr.starts_with("ipc:") {
            return Box::new(IpcTransport);
        }
        #[cfg(feature = "tcp")]
        {
            return Box::new(TcpTransport);
        }
        #[cfg(not(feature = "tcp"))]
        {
            let _ = addr;
            panic!("no transport feature enabled (enable tcp/ws/ipc)");
        }
    }
}
