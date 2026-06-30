//! Local IPC transport via the [`interprocess`] crate (Unix domain sockets on
//! Unix, named pipes on Windows).
//!
//! Note: `interprocess`'s async support is tokio-only, so this transport
//! requires the tokio runtime. The TCP and WebSocket transports are fully
//! runtime-agnostic (tokio / async-std / smol). Address forms: `ipc:/full/path`
//! (a filesystem socket path) or `ipc:name` (a short name).

use std::io;

use async_trait::async_trait;
use interprocess::local_socket::tokio::{LocalSocketListener, LocalSocketStream};
use malkuth_core::{FramedConn, Transport, WireConn, WireListener};
use tokio_util::compat::TokioAsyncReadCompatExt;

/// Strip the `ipc:` scheme prefix.
fn name_of(addr: &str) -> String {
    addr.strip_prefix("ipc:").unwrap_or(addr).to_string()
}

/// Local IPC transport.
pub struct IpcTransport;

#[async_trait]
impl Transport for IpcTransport {
    async fn listen(&self, addr: &str) -> io::Result<Box<dyn WireListener>> {
        let name = name_of(addr);
        // Best-effort cleanup of a stale socket file on unix.
        #[cfg(unix)]
        if name.starts_with('/') {
            let _ = std::fs::remove_file(&name);
        }
        let listener = LocalSocketListener::bind(name.as_str())?;
        Ok(Box::new(IpcWireListener { listener }))
    }

    async fn connect(&self, addr: &str) -> io::Result<Box<dyn WireConn>> {
        let name = name_of(addr);
        let stream = LocalSocketStream::connect(name.as_str()).await?;
        Ok(Box::new(FramedConn::new(stream.compat())))
    }

    fn name(&self) -> &'static str {
        "ipc"
    }
}

pub struct IpcWireListener {
    listener: LocalSocketListener,
}

#[async_trait]
impl WireListener for IpcWireListener {
    async fn accept(&self) -> io::Result<Box<dyn WireConn>> {
        let stream = self.listener.accept().await?;
        Ok(Box::new(FramedConn::new(stream.compat())))
    }

    fn local_addr(&self) -> io::Result<String> {
        // interprocess doesn't always expose the bound name; report a placeholder.
        Ok("ipc:local".to_string())
    }
}
