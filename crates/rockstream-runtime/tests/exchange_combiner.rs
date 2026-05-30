//! CI property tests for the pre-shuffle combiner — one entry per registered law.
//!
//! ## Proof obligation (ROADMAP v0.30)
//!
//! For every registered merge law L, combining before sending must be
//! semantically equivalent to forwarding all rows uncombined and merging at
//! the receiver:
//!
//! ```text
//! combine_then_receive(batch, L) == merge_all_at_receiver(batch, L)
//! ```
//!
//! This property is called **uncombined-equivalence**.  The test is
//! deterministic (no random data) and is structured so that each law's
//! witness batch contains at least one key with multiple rows, triggering
//! the combining code path.
//!
//! ## Laws covered
//!
//! 1. `WeightAdd/v1`   — abelian group, i64 weight addition
//! 2. `SumCount/v1`    — abelian group, (sum, count) pair
//! 3. `MaxRegister/v1` — semilattice, per-key maximum i64
//! 4. `MinRegister/v1` — semilattice, per-key minimum i64
//! 5. `HyperLogLog/v1` — semilattice, 64-register per-byte max
//! 6. `BloomUnion/v1`  — semilattice, 32-byte bitwise OR

use rockstream_runtime::exchange::{CombineStats, PreShuffleCombiner};
use rockstream_types::laws::{
    BloomUnionV1, HyperLogLogV1, LawRegistry, MaxRegisterV1, MinRegisterV1, SumCountV1, WeightAddV1,
};
use rockstream_types::laws::{
    BLOOM_UNION_ID, HLL_ID, MAX_REGISTER_ID, MIN_REGISTER_ID, SUM_COUNT_ID, WEIGHT_ADD_ID,
};
use rockstream_types::merge_law::MergeLawId;
use std::collections::HashMap;
use std::sync::Arc;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_registry() -> Arc<LawRegistry> {
    let mut reg = LawRegistry::new();
    reg.register(Arc::new(WeightAddV1));
    reg.register(Arc::new(SumCountV1));
    reg.register(Arc::new(MaxRegisterV1));
    reg.register(Arc::new(MinRegisterV1));
    reg.register(Arc::new(HyperLogLogV1));
    reg.register(Arc::new(BloomUnionV1));
    Arc::new(reg)
}

fn make_combiner() -> PreShuffleCombiner {
    PreShuffleCombiner::new(make_registry())
}

/// Run the uncombined-equivalence proof for a given law and batch.
///
/// Steps:
/// 1. Merge-all-at-receiver: fold the batch directly into a key→value map.
/// 2. Combine-then-merge: run the pre-shuffle combiner, then fold the output.
/// 3. Assert the two maps are equal.
///
/// Returns the `CombineStats` for the combiner pass (for assertions on
/// bytes-avoided).
fn assert_uncombined_equivalence(
    law_id: MergeLawId,
    batch: Vec<(Vec<u8>, Vec<u8>)>,
) -> CombineStats {
    let combiner = make_combiner();

    // Step 1: merge all rows at receiver (ground truth).
    let receiver_state = combiner
        .merge_all(law_id, &batch)
        .expect("merge_all failed");

    // Step 2: combine, then merge the (reduced) output.
    let (combined_batch, stats) = combiner.combine(law_id, batch).expect("combine failed");

    let sender_state = combiner
        .merge_all(law_id, &combined_batch)
        .expect("merge_all on combined failed");

    // Step 3: assert equivalence.
    assert_eq!(
        sorted_map(&receiver_state),
        sorted_map(&sender_state),
        "uncombined-equivalence violated for law {law_id}"
    );

    stats
}

fn sorted_map(m: &HashMap<Vec<u8>, Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut v: Vec<_> = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    v.sort();
    v
}

// ── value constructors ────────────────────────────────────────────────────────

fn weight_bytes(w: i64) -> Vec<u8> {
    w.to_be_bytes().to_vec()
}

fn sum_count_bytes(sum: i64, count: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&sum.to_be_bytes());
    v.extend_from_slice(&count.to_be_bytes());
    v
}

fn max_register_bytes(v: i64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn min_register_bytes(v: i64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn hll_bytes(seed: u8) -> Vec<u8> {
    // 64-register sketch: spread seed across registers.
    let mut buf = vec![0u8; 64];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (seed as usize % (i + 1)) as u8;
    }
    buf
}

fn bloom_bytes(seed: u8) -> Vec<u8> {
    // 32-byte Bloom filter: spread seed across bytes.
    let mut buf = vec![0u8; 32];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = seed.wrapping_add(i as u8);
    }
    buf
}

// ── 1. WeightAdd/v1 ──────────────────────────────────────────────────────────

#[test]
fn weight_add_uncombined_equivalence() {
    // Two rows with the same key "a" → combiner should merge their weights.
    // Expected combined state: a=5, b=10.
    let key_a = b"a".to_vec();
    let key_b = b"b".to_vec();
    let batch = vec![
        (key_a.clone(), weight_bytes(3)),
        (key_a.clone(), weight_bytes(2)), // same key
        (key_b.clone(), weight_bytes(10)),
    ];
    let stats = assert_uncombined_equivalence(WEIGHT_ADD_ID, batch);
    // 3 input rows → 2 output rows after combining
    assert_eq!(stats.input_count, 3);
    assert_eq!(stats.output_count, 2);
    // bytes_avoided > 0 because one row was combined
    assert!(
        stats.bytes_avoided > 0,
        "WeightAdd: expected bytes_avoided > 0, got {}",
        stats.bytes_avoided
    );
}

#[test]
fn weight_add_bytes_avoided_documented() {
    // Benchmark witness: 100 rows keyed on 10 keys (10 duplicates each).
    let mut batch = Vec::new();
    for key_idx in 0u8..10 {
        let key = vec![key_idx];
        for _ in 0u8..10 {
            batch.push((key.clone(), weight_bytes(1)));
        }
    }
    let (_, stats) = make_combiner()
        .combine(WEIGHT_ADD_ID, batch)
        .expect("combine ok");
    assert_eq!(stats.input_count, 100);
    assert_eq!(stats.output_count, 10);
    assert!(
        stats.bytes_avoided > 0,
        "WeightAdd bytes_avoided should be > 0, got {}",
        stats.bytes_avoided
    );
    // Document the bytes avoided for this law.
    eprintln!(
        "[bytes-avoided] WeightAdd/v1: input={} output={} avoided={} bytes",
        stats.input_bytes, stats.output_bytes, stats.bytes_avoided
    );
}

// ── 2. SumCount/v1 ───────────────────────────────────────────────────────────

#[test]
fn sum_count_uncombined_equivalence() {
    let key_x = b"x".to_vec();
    let key_y = b"y".to_vec();
    let batch = vec![
        (key_x.clone(), sum_count_bytes(10, 1)),
        (key_x.clone(), sum_count_bytes(20, 2)), // same key
        (key_y.clone(), sum_count_bytes(5, 1)),
    ];
    let stats = assert_uncombined_equivalence(SUM_COUNT_ID, batch);
    assert_eq!(stats.input_count, 3);
    assert_eq!(stats.output_count, 2);
    assert!(
        stats.bytes_avoided > 0,
        "SumCount: expected bytes_avoided > 0, got {}",
        stats.bytes_avoided
    );
}

#[test]
fn sum_count_bytes_avoided_documented() {
    let mut batch = Vec::new();
    for key_idx in 0u8..10 {
        let key = vec![key_idx];
        for i in 0u8..10 {
            batch.push((key.clone(), sum_count_bytes(i as i64, 1)));
        }
    }
    let (_, stats) = make_combiner()
        .combine(SUM_COUNT_ID, batch)
        .expect("combine ok");
    assert_eq!(stats.output_count, 10);
    assert!(
        stats.bytes_avoided > 0,
        "SumCount: bytes_avoided should be > 0"
    );
    eprintln!(
        "[bytes-avoided] SumCount/v1: input={} output={} avoided={} bytes",
        stats.input_bytes, stats.output_bytes, stats.bytes_avoided
    );
}

// ── 3. MaxRegister/v1 ────────────────────────────────────────────────────────

#[test]
fn max_register_uncombined_equivalence() {
    let key_p = b"p".to_vec();
    let key_q = b"q".to_vec();
    let batch = vec![
        (key_p.clone(), max_register_bytes(7)),
        (key_p.clone(), max_register_bytes(3)), // same key, lower value
        (key_p.clone(), max_register_bytes(9)), // same key, new max
        (key_q.clone(), max_register_bytes(42)),
    ];
    let stats = assert_uncombined_equivalence(MAX_REGISTER_ID, batch);
    assert_eq!(stats.input_count, 4);
    assert_eq!(stats.output_count, 2);
    assert!(
        stats.bytes_avoided > 0,
        "MaxRegister: expected bytes_avoided > 0, got {}",
        stats.bytes_avoided
    );
}

#[test]
fn max_register_bytes_avoided_documented() {
    let mut batch = Vec::new();
    for key_idx in 0u8..10 {
        let key = vec![key_idx];
        for val in 0i64..10 {
            batch.push((key.clone(), max_register_bytes(val)));
        }
    }
    let (_, stats) = make_combiner()
        .combine(MAX_REGISTER_ID, batch)
        .expect("combine ok");
    assert_eq!(stats.output_count, 10);
    assert!(
        stats.bytes_avoided > 0,
        "MaxRegister: bytes_avoided should be > 0"
    );
    eprintln!(
        "[bytes-avoided] MaxRegister/v1: input={} output={} avoided={} bytes",
        stats.input_bytes, stats.output_bytes, stats.bytes_avoided
    );
}

// ── 4. MinRegister/v1 ────────────────────────────────────────────────────────

#[test]
fn min_register_uncombined_equivalence() {
    let key_r = b"r".to_vec();
    let batch = vec![
        (key_r.clone(), min_register_bytes(100)),
        (key_r.clone(), min_register_bytes(50)), // same key, new min
        (key_r.clone(), min_register_bytes(200)), // same key, not a min
    ];
    let stats = assert_uncombined_equivalence(MIN_REGISTER_ID, batch);
    assert_eq!(stats.input_count, 3);
    assert_eq!(stats.output_count, 1);
    assert!(
        stats.bytes_avoided > 0,
        "MinRegister: expected bytes_avoided > 0, got {}",
        stats.bytes_avoided
    );
}

#[test]
fn min_register_bytes_avoided_documented() {
    let mut batch = Vec::new();
    for key_idx in 0u8..10 {
        let key = vec![key_idx];
        for val in (0i64..10).rev() {
            batch.push((key.clone(), min_register_bytes(val)));
        }
    }
    let (_, stats) = make_combiner()
        .combine(MIN_REGISTER_ID, batch)
        .expect("combine ok");
    assert_eq!(stats.output_count, 10);
    assert!(
        stats.bytes_avoided > 0,
        "MinRegister: bytes_avoided should be > 0"
    );
    eprintln!(
        "[bytes-avoided] MinRegister/v1: input={} output={} avoided={} bytes",
        stats.input_bytes, stats.output_bytes, stats.bytes_avoided
    );
}

// ── 5. HyperLogLog/v1 ────────────────────────────────────────────────────────

#[test]
fn hyper_log_log_uncombined_equivalence() {
    let key_k = b"k".to_vec();
    let batch = vec![
        (key_k.clone(), hll_bytes(1)),
        (key_k.clone(), hll_bytes(2)), // same key, different sketch
        (b"l".to_vec(), hll_bytes(3)),
    ];
    let stats = assert_uncombined_equivalence(HLL_ID, batch);
    assert_eq!(stats.input_count, 3);
    assert_eq!(stats.output_count, 2);
    assert!(
        stats.bytes_avoided > 0,
        "HyperLogLog: expected bytes_avoided > 0, got {}",
        stats.bytes_avoided
    );
}

#[test]
fn hyper_log_log_bytes_avoided_documented() {
    let mut batch = Vec::new();
    for key_idx in 0u8..5 {
        let key = vec![key_idx];
        for sketch_seed in 0u8..4 {
            batch.push((key.clone(), hll_bytes(sketch_seed)));
        }
    }
    let (_, stats) = make_combiner().combine(HLL_ID, batch).expect("combine ok");
    assert_eq!(stats.output_count, 5);
    assert!(stats.bytes_avoided > 0, "HLL: bytes_avoided should be > 0");
    eprintln!(
        "[bytes-avoided] HyperLogLog/v1: input={} output={} avoided={} bytes",
        stats.input_bytes, stats.output_bytes, stats.bytes_avoided
    );
}

// ── 6. BloomUnion/v1 ─────────────────────────────────────────────────────────

#[test]
fn bloom_union_uncombined_equivalence() {
    let key_m = b"m".to_vec();
    let batch = vec![
        (key_m.clone(), bloom_bytes(0xAA)),
        (key_m.clone(), bloom_bytes(0x55)), // same key, complementary bits
        (b"n".to_vec(), bloom_bytes(0xFF)),
    ];
    let stats = assert_uncombined_equivalence(BLOOM_UNION_ID, batch);
    assert_eq!(stats.input_count, 3);
    assert_eq!(stats.output_count, 2);
    assert!(
        stats.bytes_avoided > 0,
        "BloomUnion: expected bytes_avoided > 0, got {}",
        stats.bytes_avoided
    );
}

#[test]
fn bloom_union_bytes_avoided_documented() {
    let mut batch = Vec::new();
    for key_idx in 0u8..5 {
        let key = vec![key_idx];
        for seed in [0x01u8, 0x10, 0x20, 0x40] {
            batch.push((key.clone(), bloom_bytes(seed)));
        }
    }
    let (_, stats) = make_combiner()
        .combine(BLOOM_UNION_ID, batch)
        .expect("combine ok");
    assert_eq!(stats.output_count, 5);
    assert!(
        stats.bytes_avoided > 0,
        "BloomUnion: bytes_avoided should be > 0"
    );
    eprintln!(
        "[bytes-avoided] BloomUnion/v1: input={} output={} avoided={} bytes",
        stats.input_bytes, stats.output_bytes, stats.bytes_avoided
    );
}

// ── cross-law: all laws produce zero bytes_avoided when no duplicates exist ──

#[test]
fn no_duplicates_no_bytes_avoided() {
    let law_ids = [
        (WEIGHT_ADD_ID, weight_bytes(1)),
        (SUM_COUNT_ID, sum_count_bytes(1, 1)),
        (MAX_REGISTER_ID, max_register_bytes(1)),
        (MIN_REGISTER_ID, min_register_bytes(1)),
        (HLL_ID, hll_bytes(1)),
        (BLOOM_UNION_ID, bloom_bytes(1)),
    ];
    let combiner = make_combiner();
    for (law_id, val) in law_ids {
        // Three distinct keys → no combining possible.
        let batch = vec![
            (b"a".to_vec(), val.clone()),
            (b"b".to_vec(), val.clone()),
            (b"c".to_vec(), val.clone()),
        ];
        let (out, stats) = combiner.combine(law_id, batch).expect("combine ok");
        assert_eq!(
            out.len(),
            3,
            "law {law_id}: all-distinct batch should have 3 output rows"
        );
        assert_eq!(
            stats.bytes_avoided, 0,
            "law {law_id}: no duplicates → bytes_avoided should be 0"
        );
    }
}

// ── loopback path produces zero network calls ─────────────────────────────────

#[test]
fn loopback_zero_network_calls() {
    use rockstream_runtime::exchange::LoopbackChannel;
    let (ch, _rx) = LoopbackChannel::new(8);
    assert_eq!(
        ch.network_call_count(),
        0,
        "loopback channel must never increment network call counter"
    );
}

// ── exchange path classifier ──────────────────────────────────────────────────

#[test]
fn classifier_elided_for_same_shard_same_worker() {
    use rockstream_runtime::exchange::ExchangeClassifier;
    use rockstream_types::exchange::ExchangePath;
    use rockstream_types::ids::{ShardId, WorkerId};

    let s = ShardId(0);
    let w = WorkerId(1);
    assert_eq!(
        ExchangeClassifier::classify_shards(s, w, s, w),
        ExchangePath::Elided
    );
}

#[test]
fn classifier_loopback_for_same_worker_different_shards() {
    use rockstream_runtime::exchange::ExchangeClassifier;
    use rockstream_types::exchange::ExchangePath;
    use rockstream_types::ids::{ShardId, WorkerId};

    let w = WorkerId(1);
    assert_eq!(
        ExchangeClassifier::classify_shards(ShardId(0), w, ShardId(1), w),
        ExchangePath::Loopback
    );
}

#[test]
fn classifier_direct_for_different_workers() {
    use rockstream_runtime::exchange::ExchangeClassifier;
    use rockstream_types::exchange::ExchangePath;
    use rockstream_types::ids::{ShardId, WorkerId};

    assert_eq!(
        ExchangeClassifier::classify_shards(ShardId(0), WorkerId(1), ShardId(1), WorkerId(2)),
        ExchangePath::Direct
    );
}
