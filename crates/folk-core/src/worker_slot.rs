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
