//! Fixed-size monotonic accounting of the actual history-lookup gate.
use serde_json::{Value, json};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub(super) enum Reason {
    Initializing,
    Current,
    InputPass,
    HeightSkew,
    Backlog,
    StaleInput,
    Gap,
    Commit,
    Persistence,
    AwaitingRecheck,
}
const REASONS: [Reason; 10] = [
    Reason::Initializing,
    Reason::Current,
    Reason::InputPass,
    Reason::HeightSkew,
    Reason::Backlog,
    Reason::StaleInput,
    Reason::Gap,
    Reason::Commit,
    Reason::Persistence,
    Reason::AwaitingRecheck,
];
impl Reason {
    fn name(self) -> &'static str {
        match self {
            Self::Initializing => "initializing",
            Self::Current => "current",
            Self::InputPass => "inputPass",
            Self::HeightSkew => "heightSkew",
            Self::Backlog => "backlog",
            Self::StaleInput => "staleInput",
            Self::Gap => "gap",
            Self::Commit => "commitInProgress",
            Self::Persistence => "persistenceFailure",
            Self::AwaitingRecheck => "awaitingRecheck",
        }
    }
}
#[derive(Clone, Copy, Default)]
struct Totals {
    entries: u64,
    total: Duration,
    max: Duration,
}
pub(super) struct Gate {
    reason: Reason,
    started: Instant,
    changed: Instant,
    transitions: u64,
    blocked_transitions: u64,
    blocked_since: Option<Instant>,
    max_blocked: Duration,
    totals: [Totals; 10],
}
impl Gate {
    pub(super) fn new(now: Instant) -> Self {
        let mut totals = [Totals::default(); 10];
        totals[Reason::Initializing as usize].entries = 1;
        Self {
            reason: Reason::Initializing,
            started: now,
            changed: now,
            transitions: 0,
            blocked_transitions: 0,
            blocked_since: Some(now),
            max_blocked: Duration::ZERO,
            totals,
        }
    }
    pub(super) fn set(&mut self, reason: Reason, now: Instant) {
        if reason == self.reason {
            return;
        }
        let elapsed = now.saturating_duration_since(self.changed);
        let old = &mut self.totals[self.reason as usize];
        old.total += elapsed;
        old.max = old.max.max(elapsed);
        self.totals[reason as usize].entries += 1;
        self.transitions += 1;
        if self.reason == Reason::Current && reason != Reason::Current {
            self.blocked_transitions += 1;
            self.blocked_since = Some(now);
        } else if reason == Reason::Current {
            if let Some(start) = self.blocked_since.take() {
                self.max_blocked = self.max_blocked.max(now.saturating_duration_since(start));
            }
        }
        self.reason = reason;
        self.changed = now;
    }
    pub(super) fn snapshot(&self, now: Instant) -> Value {
        let elapsed = now.saturating_duration_since(self.changed);
        let mut totals = self.totals;
        let current = &mut totals[self.reason as usize];
        current.total += elapsed;
        current.max = current.max.max(elapsed);
        let mut by_reason = serde_json::Map::new();
        let mut blocked = Duration::ZERO;
        for reason in REASONS {
            let t = totals[reason as usize];
            if reason != Reason::Current {
                blocked += t.total;
            }
            by_reason.insert(
                reason.name().into(),
                json!({"entries":t.entries,
                "totalUs":t.total.as_micros(),"maxUs":t.max.as_micros()}),
            );
        }
        let blocked_episode = self.blocked_since.map_or(Duration::ZERO, |start| now.saturating_duration_since(start));
        json!({"reason":self.reason.name(),"currentBlockedUs":blocked_episode.as_micros(),
            "maxBlockedUs":self.max_blocked.max(blocked_episode).as_micros(),"currentReasonUs":elapsed.as_micros(),
            "transitions":self.transitions,"blockedTransitions":self.blocked_transitions,
            "observedUs":now.saturating_duration_since(self.started).as_micros(),
            "blockedUs":blocked.as_micros(),"byReason":by_reason,
            "note":"monotonic, exclusive gate-phase durations; current interval included; session-local"})
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accounts_unsampled_transitions_and_open_interval_without_double_counting() {
        let start = Instant::now();
        let mut gate = Gate::new(start);
        for (ms, reason) in [
            (1, Reason::Current),
            (2, Reason::InputPass),
            (3, Reason::HeightSkew),
            (4, Reason::HeightSkew),
            (5, Reason::Current),
            (6, Reason::Commit),
            (8, Reason::Persistence),
            (9, Reason::Gap),
            (10, Reason::StaleInput),
            (11, Reason::Backlog),
        ] {
            gate.set(reason, start + Duration::from_millis(ms));
        }
        let now = start + Duration::from_millis(12);
        let snap = gate.snapshot(now);
        assert_eq!(snap, gate.snapshot(now));
        assert_eq!(snap["observedUs"], 12000);
        assert_eq!(snap["blockedUs"], 10000);
        assert_eq!(snap["transitions"], 9);
        assert_eq!(snap["blockedTransitions"], 2);
        assert_eq!(snap["currentBlockedUs"], 6000);
        assert_eq!(snap["maxBlockedUs"], 6000);
        assert_eq!(snap["byReason"]["heightSkew"]["entries"], 1);
        assert_eq!(snap["byReason"]["heightSkew"]["totalUs"], 2000);
        assert_eq!(snap["byReason"]["commitInProgress"]["maxUs"], 2000);
        assert_eq!(snap["byReason"].as_object().unwrap().len(), 10);
    }
}
