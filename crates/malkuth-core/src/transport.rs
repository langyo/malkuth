//! Runtime-agnostic wire-transport contracts.
//!
//! Rather than exposing raw `AsyncRead + AsyncWrite` trait objects (which are
//! awkward to use as `dyn`), a [`WireConn`] is a **framed** connection: it
//! reads/writes one JSON value per message, newline-delimited (NDJSON). The
//! generic [`FramedConn<S>`] adapts any `AsyncRead + AsyncWrite` stream —
//! `async_net::TcpStream`, an adapted tokio stream, a WebSocket byte adapter —
//! into a `WireConn` with no glue.
//!
//! Because everything sits on the `futures_io` traits, the codec and the
//! server/client in the `malkuth` crate run under tokio, async-std and smol
//! alike: only the top-level executor differs.

use std::io;

use async_trait::async_trait;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use serde_json::Value;

/// A framed, object-safe JSON-RPC connection.
///
/// Each [`read_msg`] returns one deserialized NDJSON frame, or `None` on clean
/// EOF. [`write_msg`] serializes one value followed by a newline and flushes.
#[async_trait]
pub trait WireConn: Send {
    /// Read the next message, or `None` if the peer closed cleanly.
    async fn read_msg(&mut self) -> io::Result<Option<Value>>;
    /// Write one message (newline-delimited) and flush.
    async fn write_msg(&mut self, msg: &Value) -> io::Result<()>;
}

/// A server-side listener that yields accepted [`WireConn`]s.
#[async_trait]
pub trait WireListener: Send + Sync {
    /// Accept the next inbound framed connection, or return an error.
    async fn accept(&self) -> io::Result<Box<dyn WireConn>>;
}

/// A connection factory + listener factory addressed by string.
///
/// Address schemes are interpreted by the concrete implementation in the
/// `malkuth` crate (e.g. `tcp://127.0.0.1:0`, `ws://host/path`,
/// `unix:/path/to/sock`). Registering multiple schemes behind one `Transport`
/// (or composing several) is a deployment choice.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Start listening on `addr`; the returned listener yields accepted conns.
    async fn listen(&self, addr: &str) -> io::Result<Box<dyn WireListener>>;
    /// Dial `addr` and return one framed connection.
    async fn connect(&self, addr: &str) -> io::Result<Box<dyn WireConn>>;
    /// Human-readable name of this transport (e.g. `"tcp"`, `"ws"`, `"ipc"`).
    fn name(&self) -> &'static str;
}

/// Generic NDJSON framing over any duplex stream.
///
/// Wraps a stream that is both [`AsyncRead`] and [`AsyncWrite`] and implements
/// [`WireConn`] by buffering reads until a newline and serializing writes with
/// a trailing newline + flush.
pub struct FramedConn<S> {
    stream: S,
    rd_buf: Vec<u8>,
}

impl<S> FramedConn<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Wrap a duplex stream.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            rd_buf: Vec::with_capacity(4096),
        }
    }
}

#[async_trait]
impl<S> WireConn for FramedConn<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    async fn read_msg(&mut self) -> io::Result<Option<Value>> {
        loop {
            if let Some(pos) = self.rd_buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.rd_buf.drain(..=pos).collect();
                if line.iter().all(|b| b.is_ascii_whitespace()) {
                    continue;
                }
                let val = serde_json::from_slice(&line)?;
                return Ok(Some(val));
            }
            let mut tmp = [0u8; 8192];
            let n = self.stream.read(&mut tmp).await?;
            if n == 0 {
                return if self.rd_buf.is_empty() {
                    Ok(None)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed mid-frame",
                    ))
                };
            }
            self.rd_buf.extend_from_slice(&tmp[..n]);
        }
    }

    async fn write_msg(&mut self, msg: &Value) -> io::Result<()> {
        let mut data = serde_json::to_vec(msg)?;
        data.push(b'\n');
        self.stream.write_all(&data).await?;
        self.stream.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In-memory duplex pair for testing the codec without a real socket.
    /// We use a shared pipe implemented over channels of `Vec<u8>`.
    struct PipeRead(std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<u8>>>);
    impl AsyncRead for PipeRead {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut [u8],
        ) -> std::task::Poll<io::Result<usize>> {
            let mut q = self.0.lock().unwrap();
            if q.is_empty() {
                return std::task::Poll::Ready(Ok(0));
            }
            let n = buf.len().min(q.len());
            for slot in buf.iter_mut().take(n) {
                *slot = q.pop_front().unwrap();
            }
            std::task::Poll::Ready(Ok(n))
        }
    }
    struct PipeWrite(std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<u8>>>);
    impl AsyncWrite for PipeWrite {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            let mut q = self.0.lock().unwrap();
            q.extend(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    // A duplex stream = (read side, write side) over two queues.
    struct Duplex {
        r: PipeRead,
        w: PipeWrite,
    }
    impl AsyncRead for Duplex {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut [u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::pin::Pin::new(&mut self.r).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for Duplex {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::pin::Pin::new(&mut self.w).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.w).poll_flush(cx)
        }
        fn poll_close(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.w).poll_close(cx)
        }
    }

    #[test]
    fn framed_roundtrip() {
        // Two queues: client->server and server->client.
        let c2s = std::sync::Arc::new(std::sync::Mutex::new(std::collections::VecDeque::<u8>::new()));
        let s2c = std::sync::Arc::new(std::sync::Mutex::new(std::collections::VecDeque::<u8>::new()));
        let server = Duplex {
            r: PipeRead(c2s.clone()),
            w: PipeWrite(s2c.clone()),
        };
        let client = Duplex {
            r: PipeRead(s2c.clone()),
            w: PipeWrite(c2s.clone()),
        };
        let mut s = FramedConn::new(server);
        let mut c = FramedConn::new(client);

        // We need a runtime to drive async; use a tiny block_on via futures executor.
        let mut pool = futures_util::task::LocalPool::new();
        pool.run_until(async move {
            let msg = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"ping"});
            c.write_msg(&msg).await.unwrap();
            let got = s.read_msg().await.unwrap();
            assert_eq!(got, Some(msg.clone()));
            let reply = serde_json::json!({"jsonrpc":"2.0","id":1,"result":"pong"});
            s.write_msg(&reply).await.unwrap();
            let got2 = c.read_msg().await.unwrap();
            assert_eq!(got2, Some(reply));
        });
    }
}
