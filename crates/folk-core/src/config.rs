//! Configuration loading.
//!
//! Folk reads `folk.toml` from the working directory by default, with
//! environment-variable overrides via the `FOLK_` prefix (e.g.,
//! `FOLK_WORKERS_COUNT=8`).

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::{Deserialize, Serialize};

/// Top-level Folk configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FolkConfig {
    pub server: ServerConfig,
    pub workers: WorkersConfig,
    pub log: LogConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Path to the admin RPC Unix socket. Default: `/tmp/folk.sock`.
    pub rpc_socket: String,
    /// Maximum time to wait for graceful shutdown after SIGTERM.
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            rpc_socket: "/tmp/folk.sock".into(),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkersConfig {
    /// Path to the PHP worker script (e.g., `vendor/bin/folk-worker`).
    pub script: String,
    /// PHP binary path (default: `php`).
    pub php: String,
    /// Number of worker processes.
    pub count: usize,
    /// Recycle a worker after this many requests.
    pub max_jobs: u64,
    /// Recycle a worker that has been alive longer than this.
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
    /// Recycle a worker exceeding this RSS in MB.
    pub max_memory_mb: u64,
    /// Per-request execution timeout.
    #[serde(with = "humantime_serde")]
    pub exec_timeout: Duration,
    /// Per-worker boot timeout (waits for `control.ready`).
    #[serde(with = "humantime_serde")]
    pub boot_timeout: Duration,
    /// Warm up opcache before spawning workers (default: true).
    /// Loads all files from Composer classmap into shared opcache.
    pub warmup: bool,
}

impl Default for WorkersConfig {
    fn default() -> Self {
        Self {
            script: "vendor/bin/folk-worker".into(),
            php: "php".into(),
            count: 4,
            max_jobs: 1000,
            ttl: Duration::from_secs(3600),
            max_memory_mb: 256,
            exec_timeout: Duration::from_secs(30),
            boot_timeout: Duration::from_secs(30),
            warmup: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    /// Log level filter (e.g., `info`, `debug`, `folk_core=trace`).
    pub filter: String,
    /// Output format: `text`, `json`, or `pretty`.
    pub format: LogFormat,
    /// Per-plugin log level overrides.
    /// Keys: `http`, `jobs`, `grpc`, `metrics`, `process`, `core`, `ext`.
    /// Values: `trace`, `debug`, `info`, `warn`, `error`.
    #[serde(default)]
    pub plugins: HashMap<String, String>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            filter: "info".into(),
            format: LogFormat::Text,
            plugins: HashMap::new(),
        }
    }
}

impl LogConfig {
    /// Build the effective `EnvFilter` string by combining `filter` with
    /// per-plugin overrides. Friendly plugin names are mapped to Rust crate
    /// targets automatically.
    pub fn effective_filter(&self) -> String {
        if self.plugins.is_empty() {
            return self.filter.clone();
        }

        let mut parts = vec![self.filter.clone()];
        for (plugin, level) in &self.plugins {
            let target = plugin_name_to_target(plugin);
            parts.push(format!("{target}={level}"));
        }
        parts.join(",")
    }
}

fn plugin_name_to_target(name: &str) -> &str {
    match name {
        "http" => "folk_plugin_http",
        "jobs" => "folk_plugin_jobs",
        "grpc" => "folk_plugin_grpc",
        "metrics" => "folk_plugin_metrics",
        "process" => "folk_plugin_process",
        "core" => "folk_core",
        "ext" => "folk_ext",
        _ => name,
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Text,
    Json,
    Pretty,
}

impl FolkConfig {
    /// Load config from `folk.toml` in the current directory plus environment
    /// variables prefixed with `FOLK_`. Missing file is OK; defaults are used.
    pub fn load() -> Result<Self> {
        Self::load_from(Path::new("folk.toml"))
    }

    /// Load config from a specific path. Missing file is OK; defaults are used.
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut fig = Figment::from(figment::providers::Serialized::defaults(Self::default()));
        if path.exists() {
            fig = fig.merge(Toml::file(path));
        }
        fig = fig.merge(Env::prefixed("FOLK_").split("_"));
        fig.extract().context("failed to parse Folk configuration")
    }
}
