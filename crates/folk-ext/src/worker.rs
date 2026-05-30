//! ZTS worker threads — each thread runs an independent PHP context.
//!
//! A worker thread:
//! 1. Registers with TSRM via `ZtsThreadGuard`
//! 2. Starts a PHP request (`request_startup`)
//! 3. Executes the PHP worker script (which registers handlers)
//! 4. Enters the recv/send loop via `bridge::do_recv()`/`bridge::do_send()`
//! 5. Cleans up on exit

use std::sync::mpsc;
use std::thread;

use tracing::{error, info};

use crate::bridge;
use crate::zts;

/// Spawn a ZTS worker thread that executes the given PHP script.
///
/// The worker thread receives tasks via `task_rx` and signals
/// readiness via `ready_tx`. The script is expected to call
/// `folk_worker_ready()` and then loop on `folk_worker_recv()`/`folk_worker_send()`.
pub fn spawn_zts_worker(
    worker_id: u32,
    script: String,
    task_rx: mpsc::Receiver<bridge::TaskRequest>,
    ready_tx: mpsc::SyncSender<()>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name(format!("folk-worker-{worker_id}"))
        .spawn(move || {
            if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_zts_worker(worker_id, &script, task_rx, ready_tx);
            })) {
                error!(worker_id, "ZTS worker thread panicked: {e:?}");
            }
        })
        .expect("failed to spawn worker thread")
}

fn run_zts_worker(
    worker_id: u32,
    script: &str,
    task_rx: mpsc::Receiver<bridge::TaskRequest>,
    ready_tx: mpsc::SyncSender<()>,
) {
    // 1. Register with TSRM.
    let _guard = zts::ZtsThreadGuard::new();

    // 2. Start PHP request.
    if let Err(e) = zts::request_startup() {
        error!(worker_id, error = ?e, "php_request_startup failed");
        return;
    }

    // 3. Install worker bridge channels (thread-local).
    bridge::init_worker_state(worker_id, task_rx, ready_tx);

    // 4. Restore PHP VCWD to the project root before executing the script.
    //    ZTS uses per-thread virtual CWD. Without this, frameworks that rely
    //    on getcwd() will look in the wrong directory.
    if let Some(root) = crate::project_root() {
        let _ = zts::chdir(&root.to_string_lossy());
    }

    // 5. Execute the PHP worker script.
    //    The script calls folk_worker_ready() and enters the recv/send loop.
    //    This call blocks until the script exits (channel closed).
    if let Err(e) = zts::execute_script(script) {
        error!(worker_id, error = ?e, "worker script failed");
    }

    // 5. Clean up.
    bridge::cleanup_worker_state();
    zts::request_shutdown();
    // ZtsThreadGuard drop handles ts_free_thread().

    info!(worker_id, "ZTS worker thread exiting");
}
