//! `TokioRuntime`: production runtime backed by Tokio.

use std::future::Future;
use std::time::Duration;

use crate::clock::TokioClock;
use crate::network::SimNetworkHandle;
use crate::object_store::SimObjectStoreHandle;
use crate::runtime::{BoxFuture, Runtime};

/// Production runtime using Tokio for real async I/O and wall-clock time.
pub struct TokioRuntime {
    clock: TokioClock,
    object_store: SimObjectStoreHandle,
    network: SimNetworkHandle,
    seed: u64,
}

impl TokioRuntime {
    /// Create a new TokioRuntime. The seed is stored for diagnostics but
    /// does not affect execution order in production.
    pub fn new(seed: u64) -> Self {
        Self {
            clock: TokioClock::new(),
            object_store: SimObjectStoreHandle::new(),
            network: SimNetworkHandle::new(),
            seed,
        }
    }
}

impl Runtime for TokioRuntime {
    type Clock = TokioClock;

    fn clock(&self) -> &TokioClock {
        &self.clock
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            tokio::time::sleep(duration).await;
        })
    }

    fn spawn<F>(&self, _name: &'static str, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }

    fn object_store(&self) -> &SimObjectStoreHandle {
        &self.object_store
    }

    fn network(&self) -> &SimNetworkHandle {
        &self.network
    }

    fn seed(&self) -> u64 {
        self.seed
    }

    fn is_simulation(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tokio_runtime_basics() {
        let rt = TokioRuntime::new(42);
        assert!(!rt.is_simulation());
        assert_eq!(rt.seed(), 42);

        // Object store works
        rt.object_store()
            .put("test", bytes::Bytes::from("data"))
            .unwrap();
        assert_eq!(
            rt.object_store().get("test").unwrap(),
            bytes::Bytes::from("data")
        );
    }

    #[tokio::test]
    async fn tokio_runtime_sleep() {
        let rt = TokioRuntime::new(0);
        // Should complete without hanging (very short sleep)
        rt.sleep(Duration::from_millis(1)).await;
    }
}
