//! End-to-end integration test for the folk binary.
//!
//! Requires: `php` in PATH with `msgpack` extension, `folk-sdk` installed.

use std::process::{Command, Stdio};
use std::time::Duration;

/// Path to the folk binary built by Cargo.
fn folk_binary() -> std::path::PathBuf {
    let mut p = std::env::current_exe().unwrap();
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    p.join("folk")
}

#[test]
#[ignore = "requires php + msgpack + folk-sdk installed"]
fn folk_serve_boots_and_shuts_down_cleanly() {
    let binary = folk_binary();
    assert!(binary.exists(), "folk binary not found at {binary:?}");

    let mut child = Command::new(&binary)
        .arg("serve")
        .arg("--config")
        .arg("tests/fixtures/folk.toml")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn folk binary");

    // Give the server a moment to start.
    std::thread::sleep(Duration::from_secs(2));

    // Send SIGTERM.
    #[allow(unsafe_code)]
    unsafe {
        #[allow(clippy::cast_possible_wrap)]
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }

    // Wait for exit, up to 30 seconds.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match child.try_wait().unwrap() {
            Some(status) => {
                assert!(
                    status.success(),
                    "folk exited with non-zero status: {status:?}"
                );
                break;
            },
            None if std::time::Instant::now() > deadline => {
                child.kill().unwrap();
                panic!("folk did not exit within 30 seconds of SIGTERM");
            },
            None => {
                std::thread::sleep(Duration::from_millis(100));
            },
        }
    }

    // Verify no orphaned PHP processes.
    let orphan_count = Command::new("pgrep")
        .arg("-f")
        .arg("folk-worker")
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .filter(|l| !l.is_empty())
                .count()
        })
        .unwrap_or(0);

    assert_eq!(
        orphan_count, 0,
        "found {orphan_count} orphaned folk-worker processes after shutdown"
    );
}
