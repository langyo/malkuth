//! Batteries-included service orchestrator.
//!
//! [`Supervised`] composes the pluggable pieces with sensible defaults: a shared
//! [`DrainController`], an optional [`ExitSource`] (OS signals by default when
//! the `signals` feature is on), and a set of [`DrainHook`]s run during
//! shutdown. [`Supervised::serve_rpc`] races the JSON-RPC server against the
//! exit source — **without spawning** — so it runs under any runtime.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{select, FutureExt};

use malkuth_core::{DrainController, DrainHook, ExitSource, Transport};

use crate::jsonrpc::RpcHandler;
use crate::Server;

/// A composable supervised service: drain controller + exit source + drain hooks.
pub struct Supervised {
    drain: DrainController,
    exit: Option<Arc<dyn ExitSource>>,
    hooks: Vec<Arc<dyn DrainHook>>,
    drain_budget: Duration,
}

impl Default for Supervised {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervised {
    /// Start building. Nothing is draining yet; no exit source by default
    /// (call `.signals()` or `.exit(...)` to install one).
    #[must_use]
    pub fn new() -> Self {
        Self {
            drain: DrainController::new(),
            exit: None,
            hooks: Vec::new(),
            drain_budget: Duration::from_secs(30),
        }
    }

    /// A clone of the shared drain controller — wire it into your probes /
    /// registry / worker supervisor before serving.
    pub fn drain_controller(&self) -> DrainController {
        self.drain.clone()
    }

    /// Install a custom exit source (overrides any previous).
    pub fn exit<E>(mut self, source: E) -> Self
    where
        E: ExitSource + 'static,
    {
        self.exit = Some(Arc::new(source));
        self
    }

    /// Install the default OS-signal exit source (`signals` feature).
    #[cfg(feature = "signals")]
    pub fn signals(self) -> Self {
        self.exit(crate::signals::SignalExitSource)
    }

    /// Add a drain hook run (in order) during shutdown.
    pub fn on_drain<H>(mut self, hook: H) -> Self
    where
        H: DrainHook + 'static,
    {
        self.hooks.push(Arc::new(hook));
        self
    }

    /// Budget given to each drain hook (default 30s).
    pub fn drain_budget(mut self, budget: Duration) -> Self {
        self.drain_budget = budget;
        self
    }

    /// Serve JSON-RPC on `addr` until the exit source fires (or the server
    /// ends), then run the drain hooks. Runs under any runtime (no spawn).
    pub async fn serve_rpc<H>(self, transport: &dyn Transport, addr: &str, handler: Arc<H>) -> std::io::Result<()>
    where
        H: RpcHandler + ?Sized + 'static,
    {
        let ctrl = self.drain.clone();
        let server_fut = Server::serve(transport, addr, handler);
        match self.exit {
            None => {
                let _ = server_fut.await;
            }
            Some(exit) => {
                let exit_fut = exit.wait(ctrl.clone());
                select! {
                    _r = server_fut.fuse() => {}
                    _ = exit_fut.fuse() => {}
                }
            }
        }
        for hook in self.hooks {
            let _ = hook.drain(self.drain_budget).await;
        }
        Ok(())
    }
}
