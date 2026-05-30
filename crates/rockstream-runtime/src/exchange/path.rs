//! Exchange path classifier.
//!
//! `ExchangeClassifier` maps source/target worker identities to an
//! `ExchangePath`.  The decision tree is:
//!
//! 1. Same shard, same operator instance → `Elided`
//! 2. Same worker → `Loopback`
//! 3. Different worker → `Direct` (may be downgraded to `Durable` at runtime)

use rockstream_types::exchange::ExchangePath;
use rockstream_types::ids::{ShardId, WorkerId};

/// Classifies exchange routing paths from worker topology.
///
/// The classifier is stateless: each call to [`classify`] and
/// [`classify_shards`] evaluates the rule set from scratch against the
/// supplied identities.
pub struct ExchangeClassifier;

impl ExchangeClassifier {
    /// Classify a worker-to-worker exchange.
    ///
    /// * Same worker → [`ExchangePath::Loopback`]
    /// * Different workers → [`ExchangePath::Direct`]
    pub fn classify(source_worker: WorkerId, target_worker: WorkerId) -> ExchangePath {
        if source_worker == target_worker {
            ExchangePath::Loopback
        } else {
            ExchangePath::Direct
        }
    }

    /// Classify a shard-level exchange, handling the elided case.
    ///
    /// If source and target shards are the same *and* they share the same
    /// worker the exchange can be elided entirely (no channel needed).
    pub fn classify_shards(
        source_shard: ShardId,
        source_worker: WorkerId,
        target_shard: ShardId,
        target_worker: WorkerId,
    ) -> ExchangePath {
        if source_shard == target_shard && source_worker == target_worker {
            ExchangePath::Elided
        } else {
            Self::classify(source_worker, target_worker)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::{ShardId, WorkerId};

    #[test]
    fn same_worker_is_loopback() {
        let w = WorkerId(1);
        assert_eq!(ExchangeClassifier::classify(w, w), ExchangePath::Loopback);
    }

    #[test]
    fn different_workers_is_direct() {
        let w1 = WorkerId(1);
        let w2 = WorkerId(2);
        assert_eq!(ExchangeClassifier::classify(w1, w2), ExchangePath::Direct);
    }

    #[test]
    fn same_shard_same_worker_is_elided() {
        let s = ShardId(0);
        let w = WorkerId(1);
        assert_eq!(
            ExchangeClassifier::classify_shards(s, w, s, w),
            ExchangePath::Elided
        );
    }

    #[test]
    fn same_shard_different_worker_is_loopback_or_direct() {
        // Two workers each think they own the shard – this is unusual but
        // the classifier doesn't gate on ownership; it only gates on worker id.
        let s = ShardId(0);
        let w1 = WorkerId(1);
        let w2 = WorkerId(2);
        assert_eq!(
            ExchangeClassifier::classify_shards(s, w1, s, w2),
            ExchangePath::Direct
        );
    }

    #[test]
    fn different_shards_same_worker_is_loopback() {
        let s1 = ShardId(0);
        let s2 = ShardId(1);
        let w = WorkerId(1);
        assert_eq!(
            ExchangeClassifier::classify_shards(s1, w, s2, w),
            ExchangePath::Loopback
        );
    }
}
