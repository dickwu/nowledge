use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use tokio::{sync::watch, task::JoinHandle};

#[derive(Clone)]
pub(super) struct RuntimeSupervisor {
    inner: Arc<RuntimeSupervisorInner>,
}

struct RuntimeSupervisorInner {
    shutdown: watch::Sender<bool>,
    state: Mutex<RuntimeState>,
}

#[derive(Default)]
struct RuntimeState {
    closing: bool,
    handles: Vec<JoinHandle<()>>,
}

impl RuntimeSupervisor {
    pub(super) fn new() -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            inner: Arc::new(RuntimeSupervisorInner {
                shutdown,
                state: Mutex::new(RuntimeState::default()),
            }),
        }
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<bool> {
        self.inner.shutdown.subscribe()
    }

    pub(super) fn spawn<F>(&self, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closing {
            return false;
        }
        state.handles.retain(|handle| !handle.is_finished());
        state.handles.push(tokio::spawn(future));
        true
    }

    pub(super) fn begin_shutdown(&self) {
        let should_signal = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.closing {
                false
            } else {
                state.closing = true;
                true
            }
        };
        if should_signal {
            let _ = self.inner.shutdown.send(true);
        }
    }

    pub(super) async fn shutdown_until(&self, deadline: tokio::time::Instant) {
        self.begin_shutdown();
        let mut handles = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut state.handles)
        };
        if handles.is_empty() {
            return;
        }

        let joined = tokio::time::timeout_at(deadline, async {
            for handle in &mut handles {
                let _ = handle.await;
            }
        })
        .await;
        if joined.is_err() {
            for handle in &handles {
                handle.abort();
            }
            for mut handle in handles {
                if tokio::time::timeout_at(deadline, &mut handle)
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }

    #[cfg(test)]
    fn tracked_handle_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .handles
            .len()
    }

    #[cfg(test)]
    fn all_tracked_handles_finished(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .handles
            .iter()
            .all(JoinHandle::is_finished)
    }
}

impl Drop for RuntimeSupervisorInner {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for handle in state.handles.drain(..) {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn spawn_reaps_completed_handles_before_tracking_new_work() {
        let runtime = RuntimeSupervisor::new();
        for _ in 0..128 {
            assert!(runtime.spawn(async {}));
        }
        for _ in 0..1_000 {
            if runtime.all_tracked_handles_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(runtime.all_tracked_handles_finished());

        assert!(runtime.spawn(async { tokio::task::yield_now().await }));
        assert!(runtime.tracked_handle_count() <= 1);
        runtime
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
    }

    #[tokio::test]
    async fn shutdown_does_not_wait_past_an_expired_deadline_after_abort() {
        let runtime = RuntimeSupervisor::new();
        assert!(runtime.spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }));

        let started = tokio::time::Instant::now();
        runtime.shutdown_until(started).await;
        assert!(started.elapsed() < Duration::from_millis(100));
    }
}
