//! Runtime-agnostic JSON-RPC server.
//!
//! [`Server::serve`] drives an accept loop over a [`Transport`] and handles
//! every connection concurrently **without spawning** — it multiplexes them
//! with a [`FuturesUnordered`], so the whole thing is driven by whichever
//! executor the caller used to `await` `serve`. No `tokio::spawn` /
//! `async_std::task::spawn` anywhere → runs under any runtime.

use std::sync::Arc;

use futures_util::stream::{FuturesUnordered, StreamExt};
use futures_util::{select, FutureExt};
use tracing::{debug, warn};

use malkuth_core::{Transport, WireConn, WireListener};

use crate::jsonrpc::{Id, Request, Response, RpcHandler};

/// A JSON-RPC server. Stateless beyond the handler it wraps.
pub struct Server;

impl Server {
    /// Accept connections on `transport.listen(addr)` forever, dispatching each
    /// request through `handler`. Returns only on a fatal accept error.
    ///
    /// Concurrency is multiplexed (not spawned) — suitable for a moderate number
    /// of long-lived supervision connections (worker heartbeats, control RPCs).
    pub async fn serve<H>(
        transport: &dyn Transport,
        addr: &str,
        handler: Arc<H>,
    ) -> std::io::Result<()>
    where
        H: RpcHandler + ?Sized,
    {
        let listener: Box<dyn WireListener> = transport.listen(addr).await?;
        let mut conns: FuturesUnordered<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>> =
            FuturesUnordered::new();

        loop {
            // Race accept against the completion of an existing connection.
            select! {
                res = listener.accept().fuse() => match res {
                    Ok(conn) => {
                        let h = handler.clone();
                        conns.push(Box::pin(async move { serve_conn(conn, h).await; })
                            as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>);
                    }
                    Err(e) => warn!(error = %e, "accept failed"),
                },
                done = conns.next() => { let _ = done; }
            }
            // Opportunistically drain already-finished connections (bursts).
            while !conns.is_empty() {
                match conns.next().now_or_never() {
                    Some(Some(())) => {}
                    _ => break,
                }
            }
        }
    }
}

async fn serve_conn<H>(mut conn: Box<dyn WireConn>, handler: Arc<H>)
where
    H: RpcHandler + ?Sized,
{
    loop {
        match conn.read_msg().await {
            Ok(Some(value)) => {
                let req: Request = match serde_json::from_value(value) {
                    Ok(r) => r,
                    Err(e) => {
                        debug!(error = %e, "malformed request frame");
                        continue;
                    }
                };
                let is_call = req.id.is_some();
                let id = req.id.clone().unwrap_or(Id::Null);
                let outcome = handler.handle(&req).await;
                if is_call {
                    let resp = match outcome {
                        Ok(v) => Response::ok(id, v),
                        Err(e) => Response::err(id, e),
                    };
                    let resp_val = serde_json::to_value(&resp).unwrap_or_else(|_| {
                        serde_json::json!({"jsonrpc":"2.0","id":null,"error":{"code":-32000,"message":"encode failed"}})
                    });
                    if let Err(e) = conn.write_msg(&resp_val).await {
                        debug!(error = %e, "write failed; closing conn");
                        break;
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                debug!(error = %e, "conn read error; closing");
                break;
            }
        }
    }
}
