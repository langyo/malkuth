//! Internal test/reference binary for malkuth.
//!
//! Three modes (parsed from argv, no clap to stay dep-light):
//!
//! - `worker` — a single supervised unit. Listens on `$PORT`, reports `$GEN`,
//!   speaks a tiny line protocol (`ping`→`pong`, `health`→`port=..;gen=..;pid=..`,
//!   `crash`→exit(1)). This is what gets replicated/wrapped.
//! - `supervise --pods N --port-base B` — uses `malkuth::worker::Supervisor` to
//!   run N copies of itself (self-replication); OTP restart on crash.
//! - `rolling --pods N --port-base B` — runs gen-0 pods, then performs a
//!   *gradual* gray update to gen-1 (one pod at a time: bring up new, drain old)
//!   using a per-pod `DrainController` + `Supervisor`.

use std::env;
use std::process::Stdio;
use std::time::{Duration, Instant};

use malkuth::worker::{Supervisor, WorkerSpec};
use malkuth_core::{DrainController, RestartPolicy, ShutdownKind};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("");
    match mode {
        "worker" => worker().await,
        "supervise" => supervise(pods(&args), port_base(&args)).await,
        "rolling" => rolling(pods(&args), port_base(&args)).await,
        other => {
            eprintln!("usage: malkuth-test-app worker | supervise --pods N --port-base B | rolling --pods N --port-base B");
            eprintln!("  (got: {other:?})");
            std::process::exit(2);
        }
    }
}

fn pods(args: &[String]) -> usize {
    flag(args, "--pods").and_then(|v| v.parse().ok()).unwrap_or(3)
}
fn port_base(args: &[String]) -> u16 {
    flag(args, "--port-base").and_then(|v| v.parse().ok()).unwrap_or(0)
}
fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if let Some(rest) = a.strip_prefix(name) {
            if rest.is_empty() {
                return it.next().map(String::as_str);
            }
            if let Some(v) = rest.strip_prefix('=') {
                return Some(v);
            }
        }
    }
    None
}

// ── worker mode ────────────────────────────────────────────────

async fn worker() {
    let port: u16 = env::var("PORT").ok().and_then(|p| p.parse().ok()).expect("PORT env required");
    let gen: u64 = env::var("GEN").ok().and_then(|g| g.parse().ok()).unwrap_or(0);
    eprintln!("WORKER_READY port={port} gen={gen} pid={}", std::process::id());
    let listener = match TcpListener::bind(("127.0.0.1", port)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("WORKER_BIND_FAIL port={port} error={e}");
            std::process::exit(1);
        }
    };
    loop {
        let (sock, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("WORKER_ACCEPT_FAIL error={e}");
                continue;
            }
        };
        tokio::spawn(async move { handle_client(sock, port, gen).await; });
    }
}

async fn handle_client(stream: TcpStream, port: u16, gen: u64) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        let n = match reader.read_line(&mut line).await {
            Ok(n) => n,
            Err(_) => return,
        };
        if n == 0 {
            return;
        }
        let cmd = line.trim();
        let reply: Vec<u8> = match cmd {
            "ping" => b"pong\n".to_vec(),
            "health" => format!("port={port};gen={gen};pid={}\n", std::process::id()).into_bytes(),
            "crash" => {
                eprintln!("WORKER_CRASH port={port} (requested)");
                std::process::exit(1);
            }
            other => format!("err: unknown command {other:?}\n").into_bytes(),
        };
        if reader.get_mut().write_all(&reply).await.is_err() {
            return;
        }
    }
}

// ── supervise mode ─────────────────────────────────────────────

async fn supervise(pods: usize, port_base: u16) {
    let exe = env::current_exe().expect("current_exe");
    let mut specs = Vec::new();
    for i in 0..pods {
        let port = port_base + 1 + i as u16;
        specs.push(
            WorkerSpec::new(format!("w{i}"), "app", exe.to_string_lossy().to_string())
                .args(["worker"])
                .env("PORT", port.to_string())
                .env("GEN", "0")
                .policy(RestartPolicy::Permanent),
        );
    }
    eprintln!("SUPERVISE_START pods={pods} port_base={port_base}");
    let drain = DrainController::new();
    let sup = Supervisor::new(specs);
    tokio::select! {
        _ = tokio::signal::ctrl_c() => { eprintln!("SUPERVISE_STOP signal"); drain.begin_drain(ShutdownKind::Graceful); }
        infos = sup.run(drain.clone()) => {
            for i in infos { eprintln!("SUPERVISE_FINAL {i:?}"); }
            return;
        }
    }
    // ctrl_c path: run drain to completion
    // (re-run is not possible; Supervisor::run consumed specs — so just exit)
    let _ = drain;
    eprintln!("SUPERVISE_EXIT");
}

// ── rolling mode (gradual gray update) ─────────────────────────

struct Pod {
    port: u16,
    ctrl: DrainController,
    task: JoinHandle<()>,
}

fn spawn_pod(exe: &str, port: u16, gen: u64) -> Pod {
    let spec = WorkerSpec::new(format!("pod-{port}"), "app", exe.to_string())
        .args(["worker"])
        .env("PORT", port.to_string())
        .env("GEN", gen.to_string())
        .policy(RestartPolicy::Permanent);
    let ctrl = DrainController::new();
    let run_ctrl = ctrl.clone();
    let task = tokio::spawn(async move {
        let infos = Supervisor::new(vec![spec]).run(run_ctrl).await;
        for i in infos {
            eprintln!("POD_FINAL {i:?}");
        }
    });
    Pod { port, ctrl, task }
}

async fn drain_pod(pod: Pod) {
    pod.ctrl.begin_drain(ShutdownKind::Graceful);
    let _ = pod.task.await;
}

async fn wait_healthy(port: u16, deadline: Duration) -> bool {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn rolling(pods: usize, port_base: u16) {
    let exe = env::current_exe().expect("current_exe");
    let exe = exe.to_string_lossy().to_string();
    let gen0_ports: Vec<u16> = (0..pods).map(|i| port_base + 1 + i as u16).collect();
    let gen1_ports: Vec<u16> = (0..pods).map(|i| port_base + 1 + pods as u16 + i as u16).collect();

    // gen-0 up
    let mut gen0: Vec<Pod> = Vec::new();
    for &p in &gen0_ports {
        let pod = spawn_pod(&exe, p, 0);
        gen0.push(pod);
    }
    for &p in &gen0_ports {
        if !wait_healthy(p, Duration::from_secs(10)).await {
            eprintln!("ROLLING_FAIL gen0 port={p} not healthy");
            std::process::exit(1);
        }
    }
    eprintln!("ROLLING_GEN0_READY ports={gen0_ports:?}");

    // gradual update: for each pod, bring up the gen-1 counterpart, then drain gen-0
    let mut gen1: Vec<Pod> = Vec::new();
    for i in 0..pods {
        let p1 = gen1_ports[i];
        let pod = spawn_pod(&exe, p1, 1);
        if !wait_healthy(p1, Duration::from_secs(10)).await {
            eprintln!("ROLLING_FAIL gen1 port={p1} not healthy");
            std::process::exit(1);
        }
        gen1.push(pod);
        drain_pod(gen0.remove(0)).await;
        eprintln!("ROLLING_STEP {i} gen1={p1} up, gen0 drained");
    }
    eprintln!("ROLLING_DONE gen1 serving ports={:?}", gen1.iter().map(|p| p.port).collect::<Vec<_>>());

    // run gen-1 until ctrl-c
    _ = tokio::signal::ctrl_c().await;
    eprintln!("ROLLING_STOP signal");
    for pod in gen1 {
        drain_pod(pod).await;
    }
}

// keep Stdio import used (Command referenced indirectly via WorkerSpec in malkuth);
// silence unused warnings for the manual argv parser paths.
#[allow(dead_code)]
fn _unused(_: Stdio) {}
