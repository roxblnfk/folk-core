//! Dev-mode file watcher: triggers hot reload when PHP files change.
//!
//! Watches the configured directories recursively and, after a debounce
//! window, calls [`WorkerPool::trigger_reload`] which invalidates the `OPcache`
//! and recycles recyclable workers. Disabled unless `[dev] watch = true`.
//!
//! The watcher is intended for development only. The main PHP thread (worker
//! #1) is not recyclable, so a single-worker (NTS) server cannot fully hot
//! reload — run with `workers.count > 1` (ZTS) for reliable reloads.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use notify_debouncer_mini::notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{DebounceEventResult, Debouncer, new_debouncer};
use tracing::{info, warn};

use crate::config::DevConfig;
use crate::worker_pool::WorkerPool;

/// Active file watcher. Dropping it stops watching, so the caller must keep it
/// alive for the lifetime of the server.
pub type WatchGuard = Debouncer<RecommendedWatcher>;

/// Start the dev-mode file watcher.
///
/// Must be called from within a Tokio runtime (the debounce callback spawns the
/// reload task onto the current runtime). Returns a guard that must be kept
/// alive; dropping it stops the watcher.
pub fn spawn(dev: &DevConfig, pool: Arc<WorkerPool>) -> Result<WatchGuard> {
    let handle = tokio::runtime::Handle::current();
    let extensions: Vec<String> = dev
        .watch_extensions
        .iter()
        .map(|e| e.trim_start_matches('.').to_ascii_lowercase())
        .collect();

    let mut debouncer = new_debouncer(dev.debounce, move |res: DebounceEventResult| match res {
        Ok(events) => {
            let changed = events
                .iter()
                .any(|e| matches_extension(&e.path, &extensions));
            if changed {
                let pool = pool.clone();
                handle.spawn(async move { pool.trigger_reload().await });
            }
        },
        Err(error) => {
            warn!(%error, "file watcher error");
        },
    })
    .context("failed to create file watcher")?;

    let mut watched = 0usize;
    for path in &dev.watch_paths {
        let p = Path::new(path);
        if !p.exists() {
            warn!(path, "watch path does not exist, skipping");
            continue;
        }
        debouncer
            .watcher()
            .watch(p, RecursiveMode::Recursive)
            .with_context(|| format!("failed to watch {path}"))?;
        watched += 1;
    }

    if watched == 0 {
        warn!("hot reload enabled but no watch paths exist; watcher is idle");
    } else {
        info!(
            paths = ?dev.watch_paths,
            extensions = ?dev.watch_extensions,
            debounce_ms = u64::try_from(dev.debounce.as_millis()).unwrap_or(u64::MAX),
            "hot reload watcher started"
        );
    }

    Ok(debouncer)
}

/// Whether `path`'s extension is in the watched set (case-insensitive).
fn matches_extension(path: &Path, extensions: &[String]) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .is_some_and(|ext| extensions.contains(&ext))
}
