//! Book-lock diagnostics independent of the protected book and its nested locks.
use serde_json::{Value, json};
use std::{
    ops::{Deref, DerefMut},
    panic::Location,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

#[derive(Clone, Copy)]
struct Owner {
    file: &'static str,
    line: u32,
    since: Instant,
}
#[derive(Clone, Copy)]
struct Phase {
    name: &'static str,
    since: Instant,
}
#[derive(Clone, Copy, Default)]
struct Stats {
    phase: Option<Phase>,
    owner: Option<Owner>,
    acquisitions: u64,
    max_hold_us: u64,
}
#[derive(Default)]
pub(crate) struct Diagnostics {
    stats: StdMutex<Stats>,
    waiting: AtomicUsize,
}
fn micros(since: Instant) -> u64 {
    since.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}
impl Diagnostics {
    pub(crate) fn phase(self: &Arc<Self>, name: &'static str) -> PhaseGuard {
        let previous =
            self.stats.lock().unwrap_or_else(|e| e.into_inner()).phase.replace(Phase { name, since: Instant::now() });
        PhaseGuard { diagnostics: self.clone(), previous }
    }
    pub(crate) fn snapshot(&self) -> Value {
        let stats = *self.stats.lock().unwrap_or_else(|e| e.into_inner());
        let held = stats.owner.as_ref().map_or(0, |owner| micros(owner.since));
        json!({"owner":stats.owner.as_ref().map(|owner|json!({"file":owner.file,"line":owner.line})),
            "phase":stats.phase.map(|phase|json!({"name":phase.name,"elapsedUs":micros(phase.since)})),
            "heldUs":held,"maxHoldUs":stats.max_hold_us.max(held),"acquisitions":stats.acquisitions,
            "waiters":self.waiting.load(Ordering::Relaxed),
            "note":"monotonic session-local book-lock timing; owner location requires matching source revision; not a readiness assertion"})
    }
}
pub(crate) struct PhaseGuard {
    diagnostics: Arc<Diagnostics>,
    previous: Option<Phase>,
}
impl Drop for PhaseGuard {
    fn drop(&mut self) {
        self.diagnostics.stats.lock().unwrap_or_else(|e| e.into_inner()).phase = self.previous;
    }
}
pub(crate) struct Mutex<T> {
    inner: tokio::sync::Mutex<T>,
    diagnostics: Arc<Diagnostics>,
}
struct Waiter<'a>(&'a AtomicUsize);
impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
impl<T> Mutex<T> {
    pub(crate) fn new(value: T) -> Self {
        Self { inner: tokio::sync::Mutex::new(value), diagnostics: Arc::new(Diagnostics::default()) }
    }
    pub(crate) fn diagnostics(&self) -> Arc<Diagnostics> {
        self.diagnostics.clone()
    }
    // Capture the caller before constructing the future; track_caller on an
    // async fn would report the poll site instead of the acquisition site.
    #[track_caller]
    pub(crate) fn lock(&self) -> impl Future<Output = Guard<'_, T>> {
        let caller = Location::caller();
        async move {
            self.diagnostics.waiting.fetch_add(1, Ordering::Relaxed);
            let waiting = Waiter(&self.diagnostics.waiting);
            let inner = self.inner.lock().await;
            drop(waiting);
            self.acquired(inner, caller)
        }
    }
    #[cfg(test)]
    #[track_caller]
    pub(crate) fn try_lock(&self) -> Result<Guard<'_, T>, tokio::sync::TryLockError> {
        self.inner.try_lock().map(|inner| self.acquired(inner, Location::caller()))
    }
    fn acquired<'a>(
        &'a self,
        inner: tokio::sync::MutexGuard<'a, T>,
        caller: &'static Location<'static>,
    ) -> Guard<'a, T> {
        let mut stats = self.diagnostics.stats.lock().unwrap_or_else(|e| e.into_inner());
        stats.acquisitions += 1;
        stats.phase = None;
        stats.owner = Some(Owner {
            // Never expose build machine paths in diagnostics.
            file: caller.file().rsplit(['/', '\\']).next().unwrap_or("unknown"),
            line: caller.line(),
            since: Instant::now(),
        });
        Guard { inner, diagnostics: &self.diagnostics }
    }
}
pub(crate) struct Guard<'a, T> {
    inner: tokio::sync::MutexGuard<'a, T>,
    diagnostics: &'a Diagnostics,
}
impl<T> Deref for Guard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}
impl<T> DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}
impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        // Clear metadata before releasing inner; a following owner cannot be
        // erased by the departing guard. No async work or logging under stats.
        let mut stats = self.diagnostics.stats.lock().unwrap_or_else(|e| e.into_inner());
        stats.phase = None;
        if let Some(owner) = stats.owner.take() {
            stats.max_hold_us = stats.max_hold_us.max(micros(owner.since));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    #[tokio::test]
    async fn cancelling_owner_restores_phase_and_releases_lock() {
        let lock = Mutex::new(1);
        let diagnostics = lock.diagnostics();
        let mut holding = Box::pin(async {
            let _owner = lock.lock().await;
            let _phase = diagnostics.phase("blockedWork");
            futures_util::future::pending::<()>().await;
        });
        assert!(holding.as_mut().now_or_never().is_none());
        assert_eq!(diagnostics.snapshot()["phase"]["name"], "blockedWork");
        drop(holding);
        let snapshot = diagnostics.snapshot();
        assert!(snapshot["owner"].is_null());
        assert!(snapshot["phase"].is_null());
        assert!(lock.try_lock().is_ok());
    }
    #[tokio::test]
    async fn stalled_owner_is_observable_and_cancelled_waiter_is_removed() {
        let lock = Mutex::new(1);
        let diagnostics = lock.diagnostics();
        let expected_line = line!() + 1;
        let owner = lock.lock().await;
        let mut waiting = Box::pin(lock.lock());
        assert!(waiting.as_mut().now_or_never().is_none());
        let phase = diagnostics.phase("snapshot");
        {
            let _nested = diagnostics.phase("nestedWork");
            assert_eq!(diagnostics.snapshot()["phase"]["name"], "nestedWork");
        }
        assert_eq!(diagnostics.snapshot()["phase"]["name"], "snapshot");
        let snapshot = diagnostics.snapshot();
        assert_eq!(snapshot["owner"]["file"], "listener_lock.rs");
        assert_eq!(snapshot["owner"]["line"], expected_line);
        assert_eq!(snapshot["acquisitions"], 1);
        assert_eq!(snapshot["waiters"], 1);
        assert!(diagnostics.snapshot()["heldUs"].as_u64().unwrap() >= snapshot["heldUs"].as_u64().unwrap());
        drop(waiting);
        assert_eq!(diagnostics.snapshot()["waiters"], 0);
        drop(phase);
        assert!(diagnostics.snapshot()["phase"].is_null());
        drop(owner);
        assert!(diagnostics.snapshot()["owner"].is_null());
        let _next = lock.lock().await;
        assert_eq!(diagnostics.snapshot()["acquisitions"], 2);
    }
}
