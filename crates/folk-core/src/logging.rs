//! `tracing` initialization.
//!
//! Three formats supported:
//! - `text` — compact, `[plugin]` prefix, single-line (default).
//! - `json` — structured, one JSON object per line (for log aggregators).
//! - `pretty` — multi-line human format (good for development).

use anyhow::{Context, Result};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use crate::config::{LogConfig, LogFormat};

/// Initialize the global `tracing` subscriber.
///
/// Reads `RUST_LOG` first; if absent, uses `config.effective_filter()` which
/// combines `config.filter` with per-plugin overrides from `[log.plugins]`.
///
/// Should be called exactly once at server startup. Calling twice is harmless
/// but logs a warning.
pub fn init(config: &LogConfig) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.effective_filter()));

    let registry = tracing_subscriber::registry().with(filter);

    let result = match config.format {
        LogFormat::Text => registry
            .with(
                fmt::layer()
                    .with_target(true)
                    .with_thread_ids(false)
                    .with_thread_names(false),
            )
            .try_init(),
        LogFormat::Json => registry
            .with(fmt::layer().json().with_target(true).flatten_event(true))
            .try_init(),
        LogFormat::Pretty => registry.with(fmt::layer().pretty()).try_init(),
    };

    result.context("tracing subscriber already initialized")
}
