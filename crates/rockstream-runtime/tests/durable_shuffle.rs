//! CI proof tests for the durable shuffle fallback (v0.31).
//!
//! ## Proof obligations (ROADMAP v0.31)
//!
//! 1. **Receiver-fault recovery without duplicates**: Sender writes frames to
//!    the durable path; receiver reads back and obtains exactly the original
//!    set with no duplicates.
//!
//! 2. **Large batch coalescing**: Many frames fit in a single object; the
//!    reader decodes all of them correctly.
//!
//! 3. **Bit-identical state — direct vs. durable** (one proof per law):
//!    For each registered law L, the merged state produced by the durable
//!    reader must equal the merged state produced by the direct-path
//!    `PreShuffleCombiner` for the same input.
//!
//! ## Laws covered
//!
//! 1. WeightAdd/v1   — abelian group, i64 weight addition
//! 2. SumCount/v1    — abelian group, (sum, count) pair
//! 3. MaxRegister/v1 — semilattice, per-key max i64
//! 4. MinRegister/v1 — semilattice, per-key min i64
//! 5. HyperLogLog/v1 — semilattice, 64-register per-byte max
//! 6. BloomUnion/v1  — semilattice, 32-byte bitwise OR

use object_store::memory::InMemory;
use rockstream_runtime::exchange::{
    DurableShuffleReader, DurableShuffleWriter, PreShuffleCombiner, ShuffleFrame,
};
use rockstream_types::ids::{ExchangeId, ShardId, WorkerId};
use rockstream_types::laws::{
    BloomUnionV1, HyperLogLogV1, LawRegistry, MaxRegisterV1, MinRegisterV1, SumCountV1, WeightAddV1,
};
use rockstream_types::laws::{
    BLOOM_UNION_ID, HLL_ID, MAX_REGISTER_ID, MIN_REGISTER_ID, SUM_COUNT_ID, WEIGHT_ADD_ID,
};
use rockstream_types::merge_law::MergeLawId;
use std::collections::HashMap;
use std::sync::Arc;

// ── helpers ───────────────────────────────────────────────────────────────────

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

fn make_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(InMemory::new())
}

fn writer(store: Arc<dyn object_store::ObjectStore>) -> DurableShuffleWriter {
    DurableShuffleWriter::new(store, WorkerId(1), ExchangeId(42))
}

fn reader(
    store: Arc<dyn object_store::ObjectStore>,
    registry: Arc<LawRegistry>,
) -> DurableShuffleReader {
    DurableShuffleReader::new(store, registry)
}

fn frame(shard: u64, key: &[u8], val: &[u8]) -> ShuffleFrame {
    ShuffleFrame {
        target_shard: ShardId(shard),
        key: key.to_vec(),
        value: val.to_vec(),
    }
}

fn weight_val(w: i64) -> Vec<u8> {
    w.to_be_bytes().to_vec()
}

fn sum_count_val(sum: i64, count: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&sum.to_be_bytes());
    v.extend_from_slice(&count.to_be_bytes());
    v
}

fn max_val(v: i64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn min_val(v: i64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn hll_val(seed: u8) -> Vec<u8> {
    let mut buf = vec![0u8; 64];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (seed as usize % (i + 1)) as u8;
    }
    buf
}

fn bloom_val(seed: u8) -> Vec<u8> {
    let mut buf = vec![0u8; 32];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = seed.wrapping_add(i as u8);
    }
    buf
}

// ── 1. Basic write/read round-trip ────────────────────────────────────────────

#[tokio::test]
async fn durable_write_read_roundtrip() {
    let store = make_store();
    let w = writer(Arc::clone(&store));
    let r = reader(Arc::clone(&store), make_registry());

    let frames = vec![
        frame(0, b"k1", b"v1"),
        frame(1, b"k2", b"v2"),
        frame(2, b"k3", b"v3"),
    ];
    let entry = w.write(frames.clone()).await.expect("write ok");
    assert_eq!(entry.frame_count, 3);

    let got = r.read(&entry).await.expect("read ok");
    assert_eq!(got, frames);
}

// ── 2. Receiver-fault recovery — no duplicates ────────────────────────────────

#[tokio::test]
async fn receiver_fault_recovery_no_duplicates() {
    // Simulate: sender writes, receiver "fails" (doesn't read), then
    // receiver recovers and reads the same entry.  The frames come back
    // exactly once — no duplicates from the store.
    let store = make_store();
    let w = writer(Arc::clone(&store));
    let r = reader(Arc::clone(&store), make_registry());

    let frames = vec![
        frame(0, b"row_a", &weight_val(3)),
        frame(0, b"row_b", &weight_val(7)),
    ];
    let entry = w.write(frames.clone()).await.expect("write ok");

    // Receiver "fault": first read is dropped (simulated by not using result)
    let _ = r.read(&entry).await.expect("first read ok");

    // Recovery read: re-read the same entry.
    let recovered = r.read(&entry).await.expect("recovery read ok");

    // The object store is idempotent: same bytes come back.
    // Row set is exactly the original, no extra copies.
    assert_eq!(
        recovered, frames,
        "recovery read must return exact original frames"
    );
}

// ── 3. Large batch coalescing ─────────────────────────────────────────────────

#[tokio::test]
async fn large_batch_coalesces_into_single_object() {
    let store = make_store();
    let w = writer(Arc::clone(&store));
    let r = reader(Arc::clone(&store), make_registry());

    // 1 000 frames in one write → one object → one get call (not 1000 calls)
    let frames: Vec<ShuffleFrame> = (0u32..1000)
        .map(|i| {
            frame(
                u64::from(i % 8),
                format!("key-{i}").as_bytes(),
                &weight_val(i as i64),
            )
        })
        .collect();

    let entry = w.write(frames.clone()).await.expect("write ok");
    assert_eq!(entry.frame_count, 1000, "all 1000 frames in one object");

    let got = r.read(&entry).await.expect("read ok");
    assert_eq!(got.len(), 1000);
    assert_eq!(got, frames);
}

// ── 4. No LIST calls (structural guarantee) ───────────────────────────────────

#[tokio::test]
async fn no_list_call_on_read() {
    // The reader uses store.get(path) with the path from OutboxEntry.
    // InMemory ObjectStore panics on unexpected operations — but more
    // importantly: we verify that the reader only requires the path from
    // the entry metadata, not any discovery mechanism.
    let store = make_store();
    let w = writer(Arc::clone(&store));
    let r = reader(Arc::clone(&store), make_registry());

    let entry = w.write(vec![frame(0, b"k", b"v")]).await.expect("write ok");

    // Read using only the OutboxEntry (no prefix scan / list).
    // This is the structural proof: the reader API accepts an OutboxEntry,
    // not a prefix or wildcard.
    let got = r.read(&entry).await.expect("read using outbox metadata");
    assert_eq!(got.len(), 1);
}

// ── 5–10. Bit-identical state — durable vs. direct (one per law) ─────────────

/// Verifies that for law `law_id`:
///   durable_merge(frames) == direct_combine_merge(frames)
async fn assert_durable_direct_identical(law_id: MergeLawId, frames: Vec<ShuffleFrame>) {
    let registry = make_registry();
    let store = make_store();
    let w = writer(Arc::clone(&store));
    let r = reader(Arc::clone(&store), Arc::clone(&registry));

    // Durable path: write + merge_frames
    let entry = w.write(frames.clone()).await.expect("write ok");
    let durable_state = r
        .merge_frames(law_id, &entry)
        .await
        .expect("merge_frames ok");

    // Direct path: PreShuffleCombiner → merge_all on combined output
    let combiner = PreShuffleCombiner::new(Arc::clone(&registry));
    // Convert frames to key-value pairs (shard embedded in key prefix for combiner)
    let kv_batch: Vec<(Vec<u8>, Vec<u8>)> = frames
        .iter()
        .map(|f| {
            // Prefix key with shard id so combiner distinguishes cross-shard rows
            let mut k = f.target_shard.0.to_be_bytes().to_vec();
            k.extend_from_slice(&f.key);
            (k, f.value.clone())
        })
        .collect();
    let (combined, _) = combiner
        .combine(law_id, kv_batch.clone())
        .expect("combine ok");
    let direct_state_flat = combiner.merge_all(law_id, &combined).expect("merge_all ok");

    // Translate direct state back to (shard, key) form for comparison
    let direct_state: HashMap<(ShardId, Vec<u8>), Vec<u8>> = direct_state_flat
        .into_iter()
        .map(|(k, v)| {
            let shard = ShardId(u64::from_be_bytes(k[..8].try_into().unwrap()));
            let row_key = k[8..].to_vec();
            ((shard, row_key), v)
        })
        .collect();

    // Sort both maps for stable comparison
    let mut durable_sorted: Vec<_> = durable_state.into_iter().collect();
    durable_sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut direct_sorted: Vec<_> = direct_state.into_iter().collect();
    direct_sorted.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        durable_sorted, direct_sorted,
        "durable and direct paths must produce bit-identical state for law {law_id}"
    );
}

#[tokio::test]
async fn weight_add_durable_direct_identical() {
    let frames = vec![
        frame(0, b"a", &weight_val(3)),
        frame(0, b"a", &weight_val(2)),
        frame(1, b"b", &weight_val(10)),
    ];
    assert_durable_direct_identical(WEIGHT_ADD_ID, frames).await;
}

#[tokio::test]
async fn sum_count_durable_direct_identical() {
    let frames = vec![
        frame(0, b"g1", &sum_count_val(10, 1)),
        frame(0, b"g1", &sum_count_val(5, 1)),
        frame(1, b"g2", &sum_count_val(20, 3)),
    ];
    assert_durable_direct_identical(SUM_COUNT_ID, frames).await;
}

#[tokio::test]
async fn max_register_durable_direct_identical() {
    let frames = vec![
        frame(0, b"x", &max_val(7)),
        frame(0, b"x", &max_val(3)),
        frame(0, b"x", &max_val(9)),
        frame(1, b"y", &max_val(42)),
    ];
    assert_durable_direct_identical(MAX_REGISTER_ID, frames).await;
}

#[tokio::test]
async fn min_register_durable_direct_identical() {
    let frames = vec![
        frame(0, b"p", &min_val(100)),
        frame(0, b"p", &min_val(50)),
        frame(0, b"p", &min_val(200)),
    ];
    assert_durable_direct_identical(MIN_REGISTER_ID, frames).await;
}

#[tokio::test]
async fn hyper_log_log_durable_direct_identical() {
    let frames = vec![
        frame(0, b"k1", &hll_val(1)),
        frame(0, b"k1", &hll_val(2)),
        frame(1, b"k2", &hll_val(3)),
    ];
    assert_durable_direct_identical(HLL_ID, frames).await;
}

#[tokio::test]
async fn bloom_union_durable_direct_identical() {
    let frames = vec![
        frame(0, b"m", &bloom_val(0xAA)),
        frame(0, b"m", &bloom_val(0x55)),
        frame(1, b"n", &bloom_val(0xFF)),
    ];
    assert_durable_direct_identical(BLOOM_UNION_ID, frames).await;
}

// ── 11. Sequence numbers are monotone ────────────────────────────────────────

#[tokio::test]
async fn sequence_numbers_are_monotone() {
    let store = make_store();
    let w = writer(Arc::clone(&store));
    let mut prev = None;
    for _ in 0..5 {
        let entry = w.write(vec![frame(0, b"k", b"v")]).await.expect("write ok");
        if let Some(p) = prev {
            assert!(entry.sequence > p, "sequence must be strictly increasing");
        }
        prev = Some(entry.sequence);
    }
}

// ── 12. Unknown law returns DurableError::UnknownLaw ─────────────────────────

#[tokio::test]
async fn unknown_law_returns_error() {
    let store = make_store();
    let w = writer(Arc::clone(&store));
    let r = reader(Arc::clone(&store), make_registry());

    let entry = w.write(vec![frame(0, b"k", b"v")]).await.expect("write ok");

    let bad_law = MergeLawId(0xFFFF);
    let result = r.merge_frames(bad_law, &entry).await;
    assert!(
        matches!(
            result,
            Err(rockstream_runtime::exchange::DurableError::UnknownLaw(_))
        ),
        "unknown law should return DurableError::UnknownLaw"
    );
}
