//! Pod pool manager: spawns N copies of the wrapped command, each on its own
//! backend port (handed via an env var), probes readiness by TCP-connecting the
//! port, registers healthy pods with the proxy, and supports graceful rolling
//! restart (drain → SIGTERM → wait → SIGKILL → respawn).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::proxy::{Backend, ProxyState};

/// One supervised pod: the child + its assigned backend port.
struct Pod {
    child: Option<Child>,
    port: u16,
}

impl Pod {
    async fn kill(&mut self, drain_secs: u64) {
        if let Some(mut child) = self.child.take() {
            // Best-effort graceful: start_kill sends SIGTERM on unix; give the
            // process up to `drain_secs` to exit, then drop it (kill_on_drop).
            let _ = child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(drain_secs.max(1)), child.wait()).await;
        }
    }
}

/// Manages the pod pool and keeps the proxy's backend set in sync with healthy pods.
pub struct PodManager {
    host: String,
    port_env: String,
    command: Vec<String>,
    proxy: Option<Arc<ProxyState>>,
    /// Tries each pod's port until it accepts a TCP connection, up to this long.
    readiness_timeout: Duration,
    pods: Mutex<HashMap<usize, Pod>>,
    /// pod index -> its assigned port.
    ports: HashMap<usize, u16>,
    drain_secs: u64,
}

impl PodManager {
    pub fn new(
        host: String,
        port_env: String,
        command: Vec<String>,
        proxy: Option<Arc<ProxyState>>,
        ports: HashMap<usize, u16>,
        drain_secs: u64,
    ) -> Self {
        Self {
            host,
            port_env,
            command,
            proxy,
            readiness_timeout: Duration::from_secs(30),
            pods: Mutex::new(HashMap::new()),
            ports,
            drain_secs,
        }
    }

    /// Spawn all pods and begin supervising them.
    pub async fn run(self: Arc<Self>) {
        let ids: Vec<usize> = self.ports.keys().copied().collect();
        for id in ids {
            let this = Arc::clone(&self);
            tokio::spawn(async move { this.supervise(id).await });
        }
        // The supervise tasks run forever; this future just returns once spawned.
    }

    /// Restart one specific pod (used by the watcher for a rolling restart).
    pub async fn restart_one(&self, id: usize) {
        info!(pod = id, "rolling restart: draining pod");
        let mut pods = self.pods.lock().await;
        if let Some(pod) = pods.get_mut(&id) {
            pod.kill(self.drain_secs).await;
        }
        drop(pods);
        // Respawn is handled by the supervise loop's exit detection.
        // To force an immediate respawn rather than waiting for the natural
        // exit, we nudge by re-running spawn directly here:
        self.spawn_and_register(id).await;
    }

    async fn supervise(self: Arc<Self>, id: usize) {
        loop {
            self.spawn_and_register(id).await;
            // Wait for the child to exit.
            let exited = {
                let mut pods = self.pods.lock().await;
                let Some(pod) = pods.get_mut(&id) else { return };
                if let Some(child) = pod.child.as_mut() {
                    let status = child.wait().await;
                    warn!(pod = id, ?status, "pod exited");
                    true
                } else {
                    false
                }
            };
            if !exited {
                return;
            }
            // Pod gone: drop it from the proxy until it's healthy again.
            if let Some(proxy) = &self.proxy {
                proxy.set_backends(self.healthy_backends_excluding(id).await);
            }
            self.pods.lock().await.remove(&id);
            // Brief backoff before respawn to avoid a tight crash loop.
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn spawn_and_register(&self, id: usize) {
        let Some(&port) = self.ports.get(&id) else { return };
        match self.spawn_pod(id, port).await {
            Ok(child) => {
                let mut pods = self.pods.lock().await;
                pods.insert(id, Pod { child: Some(child), port });
            }
            Err(e) => {
                warn!(pod = id, error = %e, "failed to spawn pod; retrying shortly");
                sleep(Duration::from_secs(1)).await;
                return;
            }
        }
        // Probe readiness, then (re)publish the backend set.
        let ready = self.wait_ready(port).await;
        if ready {
            info!(pod = id, port, "pod ready");
            if let Some(proxy) = &self.proxy {
                proxy.set_backends(self.all_backends().await);
            }
        } else {
            warn!(pod = id, port, "pod did not become ready in time");
        }
    }

    async fn spawn_pod(&self, id: usize, port: u16) -> std::io::Result<Child> {
        let (program, args) = self.command.split_first().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "no command given")
        })?;
        let mut cmd = Command::new(program);
        cmd.args(args);
        cmd.env(&self.port_env, port.to_string());
        // A pod identity (stable per id) the program may read to self-register.
        cmd.env("MALKUTH_POD_ID", format!("pod-{id}"));
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::inherit());
        cmd.stderr(Stdio::inherit());
        cmd.kill_on_drop(true);
        info!(pod = id, port, program, "spawning pod");
        cmd.spawn()
    }

    /// Try to TCP-connect to `port` until success or the readiness timeout.
    async fn wait_ready(&self, port: u16) -> bool {
        let addr: SocketAddr = format!("{}:{}", self.host, port).parse().unwrap_or_else(|_| {
            format!("127.0.0.1:{port}").parse().unwrap()
        });
        let deadline = Instant::now() + self.readiness_timeout;
        while Instant::now() < deadline {
            if TcpStream::connect(addr).await.is_ok() {
                return true;
            }
            sleep(Duration::from_millis(150)).await;
        }
        false
    }

    async fn all_backends(&self) -> Vec<Backend> {
        let pods = self.pods.lock().await;
        self.backends_from(&pods, None)
    }

    async fn healthy_backends_excluding(&self, exclude_id: usize) -> Vec<Backend> {
        let pods = self.pods.lock().await;
        self.backends_from(&pods, Some(exclude_id))
    }

    fn backends_from(&self, pods: &HashMap<usize, Pod>, exclude: Option<usize>) -> Vec<Backend> {
        let host_ip = self
            .host
            .parse()
            .unwrap_or_else(|_| "127.0.0.1".parse().unwrap());
        pods.iter()
            .filter(|(id, _)| Some(**id) != exclude)
            .map(|(id, pod)| Backend {
                addr: SocketAddr::new(host_ip, pod.port),
                id: format!("pod-{id}"),
            })
            .collect()
    }
}

/// Assign `count` distinct ports from `ports` iterator (skipping `skip`).
pub fn assign_ports(ports: impl Iterator<Item = u16>, count: usize, skip: u16) -> HashMap<usize, u16> {
    ports
        .filter(|p| *p != skip)
        .take(count)
        .enumerate()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assign_ports_skips_public() {
        // range 3000..=3005, want 3, skip public 3000 → 3001,3002,3003
        let m = assign_ports(3000..=3005, 3, 3000);
        assert_eq!(m.len(), 3);
        assert_eq!(m[&0], 3001);
        assert_eq!(m[&2], 3003);
    }
}
