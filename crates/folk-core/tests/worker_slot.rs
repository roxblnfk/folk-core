use std::time::Duration;

use folk_core::config::WorkersConfig;
use folk_core::worker_slot::{SlotInfo, SlotState};

fn cfg(max_jobs: u64, ttl_secs: u64) -> WorkersConfig {
    WorkersConfig {
        max_jobs,
        ttl: Duration::from_secs(ttl_secs),
        ..WorkersConfig::default()
    }
}

#[test]
fn fresh_slot_starts_in_spawning_state() {
    let s = SlotInfo::new();
    assert_eq!(s.state, SlotState::Spawning);
    assert_eq!(s.jobs_handled, 0);
    assert!(s.pid.is_none());
}

#[test]
fn ready_transitions_to_idle_with_pid() {
    let mut s = SlotInfo::new();
    s.mark_ready(12345);
    assert_eq!(s.state, SlotState::Idle);
    assert_eq!(s.pid, Some(12345));
}

#[test]
fn busy_idle_cycles_increment_job_counter() {
    let mut s = SlotInfo::new();
    s.mark_ready(1);
    s.mark_busy();
    s.mark_idle();
    assert_eq!(s.jobs_handled, 1);
    assert_eq!(s.state, SlotState::Idle);
}

#[test]
fn should_recycle_after_max_jobs() {
    let mut s = SlotInfo::new();
    s.mark_ready(1);
    for _ in 0..3 {
        s.mark_busy();
        s.mark_idle();
    }
    assert!(s.should_recycle(&cfg(3, 999_999)));
}

#[test]
fn should_not_recycle_below_thresholds() {
    let s = SlotInfo::new();
    assert!(!s.should_recycle(&cfg(1000, 3600)));
}
