# Listener Handoff

## The problem

When a server process restarts, there is a window where no one is listening
on the port — incoming connections are dropped. For zero-downtime rolling
updates, the new process must inherit the listening socket from the old one.

## The solution: socket activation

Systemd (or a custom launcher) holds the listening socket fd open. When the
process restarts, the new process inherits the fd and can immediately accept
connections — the kernel queues them during the gap.

Malkuth's `acquire_listener` implements this in **pure Rust** (no `libsystemd`):

```rust
use malkuth::acquire_listener;

// Prefers systemd socket activation (fd inherited), falls back to a plain bind.
let listener = acquire_listener("0.0.0.0:8080").await?;
```

Enable the `socket-activation` feature:

```toml
[dependencies]
malkuth = { git = "https://github.com/celestia-island/malkuth.git", branch = "dev", features = ["socket-activation"] }
```

## How it works

systemd sets two environment variables:

| Variable | Meaning |
| --- | --- |
| `LISTEN_PID` | PID of the process that should inherit the fds (must equal ours) |
| `LISTEN_FDS` | Number of fds passed (starting at fd 3) |

Malkuth reads these, validates `LISTEN_PID == our_pid`, takes ownership of
fd 3 (`SD_LISTEN_FDS_START`), sets it to non-blocking, and wraps it in a
`tokio::net::TcpListener`.

If the variables are absent or the PID doesn't match, it falls back to
`TcpListener::bind(addr)`.

## systemd unit example

```ini
# /etc/systemd/system/myapp.socket
[Socket]
ListenStream=8080

[Install]
WantedBy=sockets.target
```

```ini
# /etc/systemd/system/myapp.service
[Service]
ExecStart=/usr/bin/myapp
# systemd passes the socket fd automatically when the socket unit is active
```

With this setup, `systemctl restart myapp` does not drop any in-flight
connections: the kernel holds them in the listen queue while the new process
starts and inherits the fd.
