//! Per-slot state for one worker.

use std::time::Instant;

use crate::config::WorkersConfig;

/// Current state of a worker slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Spawn requested; waiting for `control.ready`.
    Spawning,
    /// Ready to accept work.
    Idle,
    /// Currently handling a request.
    Busy,
    /// Recycle requested; finishing in-flight work, then will be replaced.
    Stopping,
    /// Terminated. Slot will be replaced.
    Dead,
}

/// Mutable state for a single worker slot. Owned exclusively by its supervisor task.
pub struct SlotInfo {
    pub state: SlotState,
    pub pid: Option<u32>,
    pub jobs_handled: u64,
    pub created_at: Instant,
}

impl SlotInfo {
    pub fn new() -> Self {
        Self {
            state: SlotState::Spawning,
            pid: None,
            jobs_handled: 0,
            created_at: Instant::now(),
        }
    }

    /// Returns `true` if this slot should be recycled per the configured policies.
    pub fn should_recycle(&self, cfg: &WorkersConfig) -> bool {
        if self.jobs_handled >= cfg.max_jobs {
            return true;
        }
        if self.created_at.elapsed() >= cfg.ttl {
            return true;
        }
        false
    }

    /// Transition to `Idle` after a successful boot.
    pub fn mark_ready(&mut self, pid: u32) {
        debug_assert!(matches!(self.state, SlotState::Spawning));
        self.state = SlotState::Idle;
        self.pid = Some(pid);
    }

    /// Transition to `Busy` when dispatching a request.
    pub fn mark_busy(&mut self) {
        debug_assert!(matches!(self.state, SlotState::Idle));
        self.state = SlotState::Busy;
    }

    /// Transition to `Idle` after a successful response.
    pub fn mark_idle(&mut self) {
        debug_assert!(matches!(self.state, SlotState::Busy | SlotState::Stopping));
        self.jobs_handled += 1;
        if matches!(self.state, SlotState::Busy) {
            self.state = SlotState::Idle;
        }
        // If Stopping, stay there until shutdown completes.
    }

    /// Mark the slot as terminated (worker died, runtime returned EOF, etc.).
    pub fn mark_dead(&mut self) {
        self.state = SlotState::Dead;
    }

    /// Request graceful drain.
    pub fn request_stop(&mut self) {
        if matches!(self.state, SlotState::Idle | SlotState::Busy) {
            self.state = SlotState::Stopping;
        }
    }
}

impl Default for SlotInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
}
