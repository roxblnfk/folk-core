//! ZTS (Thread-Safe) PHP FFI wrappers.
//!
//! Thin Rust wrappers around `folk_zts.c` which in turn wraps PHP TSRM macros.

#![allow(unsafe_code)]

use std::ffi::{CString, c_int};

use ext_php_rs::types::Zval;

unsafe extern "C" {
    fn folk_zts_thread_init();
    fn folk_zts_thread_shutdown();
    fn folk_zts_request_startup() -> c_int;
    fn folk_zts_request_shutdown();
    fn folk_zts_execute_script(filename: *const std::ffi::c_char) -> c_int;
    fn folk_zts_call_dispatch(
        func_name: *const std::ffi::c_char,
        method_zval: *mut Zval,
        params_zval: *mut Zval,
        retval: *mut Zval,
    ) -> c_int;
    fn folk_zts_eval_string(code: *const std::ffi::c_char) -> c_int;
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

/// Evaluate a PHP code string on the current thread.
///
/// The code must NOT include the opening `<?php` tag.
///
/// # Errors
/// Returns error if evaluation fails.
pub fn eval_string(code: &str) -> anyhow::Result<()> {
    let c_code = CString::new(code).map_err(|_| anyhow::anyhow!("code contains null byte"))?;
    let ret = unsafe { folk_zts_eval_string(c_code.as_ptr()) };
    if ret != 0 {
        Ok(())
    } else {
        anyhow::bail!("zend_eval_string failed")
    }
}

/// Execute a PHP script on the current thread.
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

/// Call a named PHP function with (method, params) arguments.
///
/// Used to call the dispatch function registered by the PHP worker script.
/// Returns the PHP function's return value as a `serde_json::Value`.
///
/// # Errors
/// Returns error if the function call fails.
pub fn call_dispatch(
    func_name: &str,
    method: &str,
    params: &serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let c_func =
        CString::new(func_name).map_err(|_| anyhow::anyhow!("func_name contains null byte"))?;

    // Convert method and params to Zvals.
    let mut method_zval = Zval::new();
    method_zval
        .set_string(method, false)
        .map_err(|e| anyhow::anyhow!("set_string failed: {e}"))?;

    let mut params_zval = crate::zval_convert::value_to_zval(params);
    let mut retval = Zval::new();

    let ret = unsafe {
        folk_zts_call_dispatch(
            c_func.as_ptr(),
            &raw mut method_zval,
            &raw mut params_zval,
            &raw mut retval,
        )
    };

    if ret != 0 {
        anyhow::bail!("call_user_function({func_name}) failed (code {ret})");
    }

    Ok(crate::zval_convert::zval_to_value(&retval))
}
