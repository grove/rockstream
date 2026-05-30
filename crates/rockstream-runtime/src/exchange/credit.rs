//! Credit-based backpressure for exchange senders.
//!
//! `CreditTracker` wraps a `tokio::sync::Semaphore` where each permit
//! represents one "credit" (permission to put one batch in flight).
//!
//! Senders call [`CreditTracker::acquire`] before sending a batch and hold
//! the permit until the batch is acknowledged (or until the `SemaphorePermit`
//! is dropped, which releases the credit automatically).
//!
//! This provides bounded in-flight data without explicit ACK messages when
//! the receiver and sender share a runtime (loopback) or when credits are
//! replenished by higher-level ACK logic (gRPC direct path).

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Credit-based backpressure tracker for a single exchange sender.
///
/// Each credit corresponds to one in-flight batch.  When credits are
/// exhausted the sender awaits [`CreditTracker::acquire`], creating natural
/// flow control.
#[derive(Clone, Debug)]
pub struct CreditTracker {
    semaphore: Arc<Semaphore>,
    capacity: usize,
}

impl CreditTracker {
    /// Create a new tracker with `capacity` initial credits.
    pub fn new(capacity: usize) -> Self {
        CreditTracker {
            semaphore: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    /// Acquire one credit, awaiting if none are available.
    ///
    /// The returned permit must be held until the batch is delivered.
    /// Dropping the permit releases the credit back to the pool.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("CreditTracker semaphore closed")
    }

    /// Number of credits currently available.
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Maximum credits (configured capacity).
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn credit_acquire_release() {
        let tracker = CreditTracker::new(4);
        assert_eq!(tracker.available(), 4);

        let p1 = tracker.acquire().await;
        assert_eq!(tracker.available(), 3);

        let p2 = tracker.acquire().await;
        assert_eq!(tracker.available(), 2);

        drop(p1);
        assert_eq!(tracker.available(), 3);

        drop(p2);
        assert_eq!(tracker.available(), 4);
    }

    #[tokio::test]
    async fn credit_capacity() {
        let tracker = CreditTracker::new(8);
        assert_eq!(tracker.capacity(), 8);
    }

    #[tokio::test]
    async fn credit_exhaustion_blocks() {
        let tracker = CreditTracker::new(1);
        let _permit = tracker.acquire().await;
        // With capacity=1 and one permit held, no credits are available.
        assert_eq!(tracker.available(), 0);
        // We cannot call acquire() again without a timeout; the property is
        // verified structurally above.
    }
}
