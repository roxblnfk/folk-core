//! # `folk-runtime-fork`
//!
//! Fork-based PHP worker runtime for Folk: warm `OPcache` via prefork.
//!
//! A prefork master PHP process boots the framework once, then forks
//! per-worker children that inherit its `OPcache`. Worker recycling
//! re-uses the master (no re-bootstrap).
//!
//! ## Platform
//!
//! This crate is Unix-only (`#![cfg(unix)]`). `SCM_RIGHTS` FD passing
//! is Linux primary, macOS experimental.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use folk_runtime_fork::{ForkConfig, ForkRuntime};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let runtime = ForkRuntime::new(ForkConfig {
//!     php: "php".into(),
//!     script: "vendor/bin/folk-worker".into(),
//!     boot_timeout: std::time::Duration::from_secs(30),
//! }).await?;
//! # Ok(())
//! # }
//! ```

#![cfg(unix)]

pub mod handle;
pub mod master;
pub mod runtime;
pub mod scm_rights;

pub use runtime::{ForkConfig, ForkRuntime};
