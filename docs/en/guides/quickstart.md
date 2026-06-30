# Quick Start

> **0.2 API.** Malkuth is now a tokio-based workspace (`malkuth-core` contracts +
> `malkuth` implementations + `malkuth-cli`). This guide shows the library and
> the CLI.

## Add the dependency

```toml
[dependencies]
malkuth = { git = "https://github.com/celestia-island/malkuth.git", branch = "dev" }
# features: tcp (default) | ws | ipc | signals (default) | worker | axum-probe |
#           file-lock | lease | pg-lock | replica | leader-follower
```

## A minimal JSON-RPC service

```rust
use std::sync::Arc;
use malkuth::{Router, Server, Supervised};
use malkuth::transport::TcpTransport;
use malkuth_core::Transport;
use serde_json::json;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let lis = TcpTransport.listen("tcp://127.0.0.1:0").await?;
    let supervised = Supervised::new().signals();
    let ctrl = supervised.drain_controller();

    let handler = Arc::new(
        Router::new()
            .lifecycle(ctrl, None)            // Lifecycle.Drain / Status / Health / Reload
            .route("ping", |_| Box::pin(async { Ok(json!("pong")) })),
    );

    supervised.serve_rpc_listener(lis, handler).await
}
```

- `Router::lifecycle` registers the standard lifecycle methods on a shared
  `DrainController` (+ optional `ProbeSink`).
- `Supervised` races the server against the OS-signal exit source, then runs any
  registered drain hooks. Replace `.signals()` with `.exit(your_impl)` to trigger
  drain from your own logic.

## Call it from a client

```rust
use malkuth::{Client};
use malkuth::transport::TcpTransport;
use malkuth_core::Transport;
use serde_json::json;

let mut c = Client::connect(&TcpTransport, "tcp://127.0.0.1:8080").await?;
let r = c.call("ping", json!({})).await?;       // → "pong"
c.notify("Lifecycle.Drain", json!({})).await?;  // → server begins graceful drain
```

## Other transports

Swap `TcpTransport` for `WsTransport` (feature `ws`, address `ws://host/path`)
or `IpcTransport` (feature `ipc`, address `ipc:/tmp/sock`); the server/client are
transport-agnostic. Or use `MultiTransport`, which dispatches by URL scheme.

## Wrap a program that does not use the library (the CLI)

```bash
malkuth --watch ./src --proxy 3000:3000-3999 --pod-count 3 -- cargo run
```

This runs 3 pods (self-assigning ports 3001–3003 via the `PORT` env var),
probes each until it is listening, and fronts them with a sticky reverse proxy on
3000 (consistent-hash routing by client IP). A change under `./src` triggers a
rolling restart, one pod at a time. See `malkuth --help` for all flags.
