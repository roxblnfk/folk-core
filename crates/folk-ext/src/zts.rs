//! ZTS (Thread-Safe) PHP FFI wrappers.
//!
//! Thin Rust wrappers around `folk_zts.c` which in turn wraps PHP TSRM macros.

#![allow(unsafe_code)]

use std::ffi::{CString, c_int};

unsafe extern "C" {
    fn folk_zts_thread_init();
    fn folk_zts_thread_shutdown();
    fn folk_zts_request_startup() -> c_int;
    fn folk_zts_request_shutdown();
    fn folk_zts_execute_script(filename: *const std::ffi::c_char) -> c_int;
    fn folk_zts_is_enabled() -> c_int;
}

/// Returns true if PHP was compiled with ZTS (thread safety).
pub fn is_zts() -> bool {
    unsafe { folk_zts_is_enabled() != 0 }
}

/// RAII guard for a PHP ZTS thread context.
///
/// Creates TSRM resources on construction, frees them on drop.
/// Must be created from a dedicated OS thread (not main PHP thread).
pub struct ZtsThreadGuard {
    _private: (),
}

impl ZtsThreadGuard {
    /// Register the current thread with PHP TSRM.
    pub fn new() -> Self {
        unsafe {
            folk_zts_thread_init();
        }
        Self { _private: () }
    }
}

impl Default for ZtsThreadGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ZtsThreadGuard {
    fn drop(&mut self) {
        unsafe {
            folk_zts_thread_shutdown();
        }
    }
}

/// Start a PHP request on the current thread.
///
/// # Errors
/// Returns error if `php_request_startup()` fails.
pub fn request_startup() -> anyhow::Result<()> {
    let ret = unsafe { folk_zts_request_startup() };
    if ret == 0 {
        Ok(())
    } else {
        anyhow::bail!("php_request_startup failed (code {ret})")
    }
}

/// Shut down the PHP request on the current thread.
pub fn request_shutdown() {
    unsafe {
        folk_zts_request_shutdown();
    }
}

/// Execute a PHP script on the current thread.
///
/// The thread must have TSRM context (via [`ZtsThreadGuard`]) and
/// an active request (via [`request_startup`]).
///
/// # Errors
/// Returns error if the script fails to execute.
pub fn execute_script(filename: &str) -> anyhow::Result<()> {
    let c_filename =
        CString::new(filename).map_err(|_| anyhow::anyhow!("filename contains null byte"))?;
    let ret = unsafe { folk_zts_execute_script(c_filename.as_ptr()) };
    if ret != 0 {
        Ok(())
    } else {
        anyhow::bail!("php_execute_script failed for {filename}")
    }
}
