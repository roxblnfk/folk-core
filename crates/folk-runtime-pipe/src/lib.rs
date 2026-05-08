//! # `folk-runtime-pipe`
//!
//! Pipe-based PHP worker runtime for Folk.
//!
//! Spawns each PHP worker via `execve`, connecting it to the Rust master
//! through two Unix socketpairs:
//!
//! - FD 3 -> task channel (request/response RPC traffic)
//! - FD 4 -> control channel (ready/idle/shutdown signals)
//!
//! Uses `pre_exec` to set up file descriptors after fork but before exec.
//!
//! ## Platform
//!
//! This crate is Unix-only (`#![cfg(unix)]`). It does not compile on Windows.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use folk_runtime_pipe::{PipeConfig, PipeRuntime};
//!
//! let runtime = PipeRuntime::new(PipeConfig {
//!     php: "php".into(),
//!     script: "vendor/bin/folk-worker".into(),
//! });
//! ```

#![cfg(unix)]

pub mod handle;
pub mod runtime;
pub mod socket;
pub mod spawn;

pub use runtime::{PipeConfig, PipeRuntime};
