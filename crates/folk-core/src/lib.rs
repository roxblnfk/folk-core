//! Folk application server core: config, worker pool, plugin registry.
//!
//! See `folk-spec/spec/01-architecture.md` for the role of this crate.

pub mod config;
pub mod health_registry;
pub mod logging;
pub mod metrics_registry;
pub mod plugin_registry;
pub mod runtime;
pub mod server;
pub mod watch;
pub mod worker_pool;
pub mod worker_slot;
