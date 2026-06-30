//! `malkuth` — watchdog-style supervisor binary.
//!
//! Wraps any program (even one that does not use the malkuth library) with a
//! supervised pod pool, optional file watching, and an L4 sticky reverse proxy.

mod cli;
mod pool;
mod proxy;
mod watcher;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::signal;
use tracing::{error, info};

use cli::{Args, ProxySpec};
use pool::{assign_ports, PodManager};
use proxy::{ProxyState, run_proxy};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    if args.command.is_empty() {
        error!("no command given — usage: malkuth [OPTIONS] -- <cmd> [args...]");
        std::process::exit(2);
    }

    // Parse proxy spec (if any) and decide the backend port range.
    let proxy_spec = match args.proxy.as_deref() {
        Some(s) => Some(ProxySpec::parse(s).unwrap_or_else(|e| {
            error!("{e}");
            std::process::exit(2);
        })),
        None => None,
    };

    // Assign backend ports to each pod.
    let ports = match &proxy_spec {
        Some(spec) => assign_ports(spec.backend_ports().collect::<Vec<_>>().into_iter(), args.pod_count, spec.public_port),
        None => {
            // No proxy: still allow N pods, but with no port assignment we just
            // run the command N times (ports not managed).
            (0..args.pod_count).map(|i| (i, 0u16)).collect()
        }
    };

    // Build proxy state (if a proxy spec was given).
    let proxy_state = proxy_spec.map(|spec| {
        Arc::new(ProxyState::new(Duration::from_secs(args.sticky_ttl_secs)))
    });
    if let Some(spec) = proxy_spec {
        info!(
            public = spec.public_port,
            range = %format!("{}-{}", spec.range_lo, spec.range_hi),
            pods = args.pod_count,
            "starting sticky reverse proxy"
        );
    }

    // Start the pod manager.
    let manager = Arc::new(PodManager::new(
        args.host.clone(),
        args.port_env.clone(),
        args.command.clone(),
        proxy_state.clone(),
        ports,
        args.drain_secs,
    ));
    Arc::clone(&manager).run().await;

    // Start the proxy (if any).
    if let Some(state) = proxy_state {
        if let Some(spec) = proxy_spec {
            let public: SocketAddr = format!("{}:{}", args.host, spec.public_port)
                .parse()
                .unwrap_or_else(|e| {
                    error!("invalid proxy bind address: {e}");
                    std::process::exit(2);
                });
            tokio::spawn(async move {
                if let Err(e) = run_proxy(public, state).await {
                    error!(error = %e, "proxy stopped");
                }
            });
        }
    }

    // Start the file watcher (if any) → rolling restart of one pod at a time.
    if !args.watch.is_empty() {
        let mut rx = watcher::spawn(args.watch.clone());
        let manager = Arc::clone(&manager);
        tokio::spawn(async move {
            let mut next_pod: usize = 0;
            while rx.recv().await.is_some() {
                // Rolling: restart one pod per change event, round-robin.
                let pod_count = args.pod_count.max(1);
                let id = next_pod % pod_count;
                next_pod = next_pod.wrapping_add(1);
                info!(pod = id, "rolling restart triggered by file change");
                manager.restart_one(id).await;
            }
        });
    }

    info!("malkuth supervisor ready; press Ctrl-C to stop");
    signal::ctrl_c().await.ok();
    info!("shutdown signal received; exiting (child pods killed via kill_on_drop)");
}
