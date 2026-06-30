//! Default [`ExitSource`] driven by OS signals (tokio::signal).
//!
//! Canonical convention:
//! - `SIGINT` / `SIGTERM` → graceful drain (exit)
//! - `SIGQUIT`           → immediate exit
//! - `SIGHUP`            → hot reload (do **not** exit; keep serving)

use async_trait::async_trait;
use malkuth_core::{DrainController, ExitReason, ExitSource, ShutdownKind};
use tracing::info;

/// OS-signal-driven exit source.
pub struct SignalExitSource;

#[cfg(unix)]
#[async_trait]
impl ExitSource for SignalExitSource {
    async fn wait(&self, ctrl: DrainController) -> ExitReason {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => { tracing::warn!(error = %e, "install SIGINT failed"); return ExitReason::graceful(); }
        };
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => { tracing::warn!(error = %e, "install SIGTERM failed"); return ExitReason::graceful(); }
        };
        let mut sighup = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => { tracing::warn!(error = %e, "install SIGHUP failed"); return ExitReason::graceful(); }
        };
        let mut sigquit = match signal(SignalKind::quit()) {
            Ok(s) => s,
            Err(e) => { tracing::warn!(error = %e, "install SIGQUIT failed"); return ExitReason::graceful(); }
        };
        loop {
            tokio::select! {
                _ = sigint.recv() => { info!("SIGINT → graceful drain"); ctrl.begin_drain(ShutdownKind::Graceful); return ExitReason::graceful(); }
                _ = sigterm.recv() => { info!("SIGTERM → graceful drain"); ctrl.begin_drain(ShutdownKind::Graceful); return ExitReason::graceful(); }
                _ = sigquit.recv() => { tracing::warn!("SIGQUIT → immediate exit"); ctrl.begin_drain(ShutdownKind::Immediate); return ExitReason::immediate(); }
                _ = sighup.recv() => { info!("SIGHUP → reload (no exit)"); ctrl.begin_drain(ShutdownKind::Reload); /* keep serving */ }
            }
        }
    }
}

#[cfg(not(unix))]
#[async_trait]
impl ExitSource for SignalExitSource {
    async fn wait(&self, ctrl: DrainController) -> ExitReason {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("ctrl_c → graceful drain");
            ctrl.begin_drain(ShutdownKind::Graceful);
        }
        ExitReason::graceful()
    }
}
