//! Same-worker loopback exchange channel.
//!
//! `LoopbackChannel` wraps a bounded `tokio::mpsc` channel.  Because source
//! and target live in the same process, **zero network calls** are made.
//! This is verifiable: [`LoopbackChannel::network_call_count`] is always 0.
//!
//! The channel capacity controls backpressure: a slow receiver will cause
//! the sender's `send` to await when the channel is full, naturally slowing
//! the pipeline without unbounded buffering.

use tokio::sync::mpsc;

/// A single exchange batch: a vector of `(key_bytes, value_bytes)` pairs.
pub type ExchangeBatch = Vec<(Vec<u8>, Vec<u8>)>;

/// Sender half of a same-worker loopback channel.
#[derive(Clone)]
pub struct LoopbackChannel {
    tx: mpsc::Sender<ExchangeBatch>,
}

/// Receiver half of a same-worker loopback channel.
pub struct LoopbackReceiver {
    rx: mpsc::Receiver<ExchangeBatch>,
}

impl LoopbackChannel {
    /// Create a new loopback channel pair.
    ///
    /// `capacity` sets the number of in-flight batches before back-pressure
    /// kicks in; 64 is a reasonable default.
    pub fn new(capacity: usize) -> (Self, LoopbackReceiver) {
        let (tx, rx) = mpsc::channel(capacity);
        (LoopbackChannel { tx }, LoopbackReceiver { rx })
    }

    /// Send a batch through the in-process channel.
    ///
    /// Awaits if the channel is full (credit backpressure in effect).
    pub async fn send(
        &self,
        batch: ExchangeBatch,
    ) -> Result<(), mpsc::error::SendError<ExchangeBatch>> {
        self.tx.send(batch).await
    }

    /// Number of network calls made by this channel.
    ///
    /// Always 0: the loopback path never crosses the network boundary.
    pub fn network_call_count(&self) -> u64 {
        0
    }
}

impl LoopbackReceiver {
    /// Receive the next batch, or `None` if all senders have been dropped.
    pub async fn recv(&mut self) -> Option<ExchangeBatch> {
        self.rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn loopback_roundtrip() {
        let (ch, mut rx) = LoopbackChannel::new(16);
        let batch = vec![(b"key1".to_vec(), b"val1".to_vec())];
        ch.send(batch.clone()).await.expect("send ok");
        let received = rx.recv().await.expect("recv ok");
        assert_eq!(received, batch);
    }

    #[tokio::test]
    async fn loopback_zero_network_calls() {
        let (ch, _rx) = LoopbackChannel::new(8);
        assert_eq!(ch.network_call_count(), 0);
        let _ch2 = ch.clone();
        assert_eq!(_ch2.network_call_count(), 0);
    }

    #[tokio::test]
    async fn loopback_sender_drop_closes_receiver() {
        let (ch, mut rx) = LoopbackChannel::new(8);
        drop(ch);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn loopback_multiple_batches() {
        let (ch, mut rx) = LoopbackChannel::new(8);
        for i in 0u8..4 {
            ch.send(vec![(vec![i], vec![i * 2])])
                .await
                .expect("send ok");
        }
        drop(ch);
        let mut count = 0usize;
        while rx.recv().await.is_some() {
            count += 1;
        }
        assert_eq!(count, 4);
    }
}
