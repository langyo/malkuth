# Worker Supervision

## The model

A **worker** is an independently-killable child process that holds exactly one
resource (a PLC connection, a serial port, a sidecar like cosmos or pglite-proxy).
The child process is the **failure-isolation boundary**: if the resource crashes,
only the worker restarts — the parent keeps serving.

## Defining workers

```rust
use malkuth::{Supervisor, WorkerSpec};
use malkuth::RestartPolicy;

let workers = vec![
    WorkerSpec::new("plc-1", "modbus", "/usr/bin/modbus-bridge")
        .args(["--device", "/dev/ttyUSB0"])
        .policy(RestartPolicy::Permanent),

    WorkerSpec::new("cosmos", "cosmos", "/usr/bin/cosmos-agent")
        .policy(RestartPolicy::Transient), // restart only on abnormal exit
];
```

## Restart policies

Borrowed from Erlang/OTP:

| Policy | Restarts on… |
| --- | --- |
| `Permanent` (default) | Any exit, even a clean one |
| `Transient` | Abnormal (non-zero) exit only |
| `Temporary` | Never |

## Rate limiting

The supervisor applies a **sliding-window rate limit** to prevent crash storms:

```rust
let supervisor = Supervisor::new(workers)
    .rate_limit(5, std::time::Duration::from_secs(60)) // max 5 restarts / 60s
    .cooldown(std::time::Duration::from_secs(30));      // then cooldown 30s
```

If a worker crashes more than `max_restarts` times within the window, it enters
cooldown before the next attempt.

## Running the supervisor

```rust
use tokio::sync::watch;

let (shutdown_tx, shutdown_rx) = watch::channel(false);

let supervisor = Supervisor::new(workers)
    .rate_limit(5, std::time::Duration::from_secs(60));

// Run until shutdown signal:
tokio::spawn(async move {
    let final_status = supervisor.run(shutdown_rx).await;
    for w in &final_status {
        tracing::info!(worker = %w.id, status = ?w.status, restarts = w.restart_count, "final");
    }
});

// Later, trigger shutdown:
let _ = shutdown_tx.send(true);
```

## Worker status snapshots

After `supervisor.run()` completes (on shutdown), it returns a `Vec<WorkerInfo>`
with each worker's final state, restart count, and last error — useful for logging
or reporting to a monitoring system.
