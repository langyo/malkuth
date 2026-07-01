//! Runtime-agnostic JSON-RPC client.
//!
//! [`Client`] holds one framed connection and issues sequential request/response
//! calls (id is a monotonic counter). Notifications send-and-forget.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;
use tracing::debug;

use crate::{Transport, WireConn};

use crate::jsonrpc::{Id, Request, Response, RpcError};

/// A simple request/response client over one framed connection.
pub struct Client {
    conn: Box<dyn WireConn>,
    next_id: AtomicU64,
}

impl Client {
    /// Connect to `addr` over `transport`.
    pub async fn connect(transport: &dyn Transport, addr: &str) -> std::io::Result<Self> {
        let conn = transport.connect(addr).await?;
        Ok(Self {
            conn,
            next_id: AtomicU64::new(1),
        })
    }

    /// Wrap an already-established framed connection.
    pub fn from_conn(conn: Box<dyn WireConn>) -> Self {
        Self {
            conn,
            next_id: AtomicU64::new(1),
        }
    }

    /// Issue a call and await its result.
    pub async fn call(&mut self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let req = Request::call(id, method, params);
        let req_val = serde_json::to_value(&req)
            .map_err(|e| RpcError::server(format!("encode request: {e}")))?;
        self.conn
            .write_msg(&req_val)
            .await
            .map_err(|e| RpcError::server(format!("write: {e}")))?;

        loop {
            let msg = self
                .conn
                .read_msg()
                .await
                .map_err(|e| RpcError::server(format!("read: {e}")))?
                .ok_or_else(|| RpcError::server("connection closed before response"))?;
            let resp: Response = serde_json::from_value(msg)
                .map_err(|e| RpcError::server(format!("decode response: {e}")))?;
            // Skip responses not matching our id (e.g. interleaved notifications
            // — though a strict call/response peer won't send any).
            if resp.id != Id::Num(id) {
                debug!(?resp.id, "ignoring mismatched response id");
                continue;
            }
            if let Some(err) = resp.error {
                return Err(err);
            }
            return Ok(resp.result.unwrap_or(Value::Null));
        }
    }

    /// Send a notification (no id, no reply expected).
    pub async fn notify(&mut self, method: &str, params: Value) -> Result<(), RpcError> {
        let req = Request::notify(method, params);
        let req_val = serde_json::to_value(&req)
            .map_err(|e| RpcError::server(format!("encode notification: {e}")))?;
        self.conn
            .write_msg(&req_val)
            .await
            .map_err(|e| RpcError::server(format!("write: {e}")))?;
        Ok(())
    }
}
