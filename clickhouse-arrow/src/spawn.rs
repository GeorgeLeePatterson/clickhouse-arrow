//! A wrapper around a `JoinHandle` that handles panics.
//!
//! Taken from [Datafusion](https://github.com/apache/datafusion/blob/ca0b760af6137c0dbec8b07daa5f48e262420cb5/datafusion/common-runtime/src/common.rs)
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::task::{JoinError, JoinHandle};

/// Helper that  provides a simple API to spawn a single task and join it.
/// Provides guarantees of aborting on `Drop` to keep it cancel-safe.
/// Note that if the task was spawned with `spawn_blocking`, it will only be
/// aborted if it hasn't started yet.
///
/// Technically, it's just a wrapper of a `JoinHandle` overriding drop.
#[derive(Debug)]
pub struct SpawnedTask<R> {
    inner: JoinHandle<R>,
}

impl<R: 'static> SpawnedTask<R> {
    pub fn spawn<T>(task: T) -> Self
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send,
    {
        // Ok to use spawn here as SpawnedTask handles aborting/cancelling the task on Drop
        #[allow(clippy::disallowed_methods)]
        let inner = tokio::task::spawn(task);
        Self { inner }
    }

    pub fn spawn_blocking<T>(task: T) -> Self
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send,
    {
        // Ok to use spawn_blocking here as SpawnedTask handles aborting/cancelling the task on Drop
        #[allow(clippy::disallowed_methods)]
        let inner = tokio::task::spawn_blocking(task);
        Self { inner }
    }

    /// Joins the task, returning the result of join (`Result<R, JoinError>`).
    /// Same as awaiting the spawned task, but left for backwards compatibility.
    ///
    /// # Errors
    /// Returns an error if the underlying task cannot be polled.
    pub async fn join(self) -> Result<R, JoinError> { self.await }

    /// Joins the task and unwinds the panic if it happens.
    ///
    /// # Errors
    /// Returns an error if the underlying task cannot be polled, the task panicked, or was
    /// cancelled.
    pub async fn join_unwind(self) -> Result<R, JoinError> {
        self.await.map_err(|e| {
            // `JoinError` can be caused either by panic or cancellation. We have to handle panics:
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                // Cancellation may be caused by two reasons:
                // 1. Abort is called, but since we consumed `self`, it's not our case (`JoinHandle`
                //    not accessible outside).
                // 2. The runtime is shutting down.
                tracing::warn!("SpawnedTask was polled during shutdown");
                e
            }
        })
    }
}

impl<R> Future for SpawnedTask<R> {
    type Output = Result<R, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<R> Drop for SpawnedTask<R> {
    fn drop(&mut self) { self.inner.abort(); }
}

#[cfg(test)]
mod tests {
    use std::future::{Pending, pending};

    use tokio::runtime::Runtime;
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn runtime_shutdown() {
        let rt = Runtime::new().unwrap();
        #[allow(clippy::async_yields_async)]
        let task = rt
            .spawn(async {
                SpawnedTask::spawn(async {
                    let fut: Pending<()> = pending();
                    fut.await;
                    unreachable!("should never return");
                })
            })
            .await
            .unwrap();

        // caller shutdown their DF runtime (e.g. timeout, error in caller, etc)
        rt.shutdown_background();

        // race condition
        // poll occurs during shutdown (buffered stream poll calls, etc)
        assert!(matches!(
            task.join_unwind().await,
            Err(e) if e.is_cancelled()
        ));
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn panic_resume() {
        // this should panic w/o an `unwrap`
        let _ = SpawnedTask::spawn(async { panic!("foo") }).join_unwind().await.ok();
    }

    #[tokio::test]
    async fn cancel_not_started_task() {
        let (sender, receiver) = oneshot::channel::<i32>();
        let task = SpawnedTask::spawn(async {
            // Shouldn't be reached.
            sender.send(42).unwrap();
        });

        drop(task);

        // If the task was cancelled, the sender was also dropped,
        // and awaiting the receiver should result in an error.
        assert!(receiver.await.is_err());
    }

    #[tokio::test]
    async fn cancel_ongoing_task() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let task = SpawnedTask::spawn(async move {
            sender.send(1).await.unwrap();
            // This line will never be reached because the channel has a buffer
            // of 1.
            sender.send(2).await.unwrap();
        });
        // Let the task start.
        assert_eq!(receiver.recv().await.unwrap(), 1);
        drop(task);

        // The sender was dropped so we receive `None`.
        assert!(receiver.recv().await.is_none());
    }
}
