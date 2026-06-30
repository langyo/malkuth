# Graceful Shutdown & Drain

## The problem

Most Rust servers only catch `ctrl_c` (SIGINT). But `docker stop`, `systemctl restart`,
and Kubernetes pod termination send **SIGTERM** — which bypasses your graceful shutdown
and kills in-flight requests.

## The solution: `DrainController`

`DrainController::install()` sets up canonical signal handlers following the
nginx/Go convention:

| Signal | Meaning | Drains? |
| --- | --- | --- |
| `SIGINT` / `SIGTERM` | Graceful shutdown | Yes |
| `SIGHUP` | Hot config reload | No (server keeps serving) |
| `SIGQUIT` | Immediate exit | Yes (skip drain) |

## Usage

```rust
use malkuth::DrainController;

let ctrl = DrainController::install();

// Pass clones to whoever needs to observe drain:
// - the serve loop (to stop accepting)
// - the probe layer (to set the /readyz draining bit)
// - background tasks (to wind down)

// Block until a drain/immediate signal fires.
let kind = ctrl.wait_for_drain().await;
```

## Wiring it into `axum::serve`

```rust
axum::serve(listener, app)
    .with_graceful_shutdown(async {
        ctrl.wait_for_drain().await;
    })
    .await?;
```

`wait_for_drain` resolves on `SIGINT`/`SIGTERM`/`SIGQUIT` but **not** on `SIGHUP`,
so a reload does not accidentally shut down the server.

## Observing drain state

```rust
// Non-blocking check:
if ctrl.is_draining() {
    // refuse new work
}

// Sleep, but wake early if drain begins:
ctrl.sleep_or_drain(std::time::Duration::from_secs(30)).await;
```

## Programmatic drain

You can also trigger drain from inside the process (e.g. an admin RPC):

```rust
ctrl.begin_drain(malkuth::ShutdownKind::Graceful);
```

## `ShutdownKind`

```rust
pub enum ShutdownKind {
    Graceful,   // SIGINT / SIGTERM — drain, then exit 0
    Immediate,  // SIGQUIT — skip drain, exit fast
    Reload,     // SIGHUP — reload config, do NOT exit
}
```
