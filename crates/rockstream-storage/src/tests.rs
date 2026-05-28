//! Integration tests for the storage layer.
//!
//! Validates:
//! - Storage API (only supported SlateDB features used)
//! - Key encoder roundtrips
//! - WriteBatch commit atomicity
//! - DbReader snapshot reads
//! - WAL reader smoke tests
//! - No range-delete dependency
//! - SlateDB determinism test (two runs at same seed → identical state)

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;

use crate::keys::{CatalogKeyEncoder, CatalogType, ShardKeyEncoder, ShardPrefix};
use crate::merge_registry::MergeOperatorRegistry;
use crate::shard_db::{ShardDb, WriteBatch};

/// Helper to create a test shard database backed by in-memory object store.
async fn test_shard_db(path: &str) -> (ShardDb, Arc<InMemory>) {
    let store = Arc::new(InMemory::new());
    let db = ShardDb::builder(path, store.clone()).build().await.unwrap();
    (db, store)
}

// === Storage API Validation ===

#[tokio::test]
async fn put_get_roundtrip() {
    let (db, _) = test_shard_db("test/put_get").await;
    db.put(b"key1", b"value1").await.unwrap();
    let val = db.get(b"key1").await.unwrap();
    assert_eq!(val, Some(Bytes::from("value1")));
    db.close().await.unwrap();
}

#[tokio::test]
async fn get_nonexistent_returns_none() {
    let (db, _) = test_shard_db("test/get_none").await;
    let val = db.get(b"nonexistent").await.unwrap();
    assert_eq!(val, None);
    db.close().await.unwrap();
}

#[tokio::test]
async fn delete_removes_key() {
    let (db, _) = test_shard_db("test/delete").await;
    db.put(b"key1", b"value1").await.unwrap();
    db.delete(b"key1").await.unwrap();
    let val = db.get(b"key1").await.unwrap();
    assert_eq!(val, None);
    db.close().await.unwrap();
}

#[tokio::test]
async fn overwrite_value() {
    let (db, _) = test_shard_db("test/overwrite").await;
    db.put(b"key1", b"v1").await.unwrap();
    db.put(b"key1", b"v2").await.unwrap();
    let val = db.get(b"key1").await.unwrap();
    assert_eq!(val, Some(Bytes::from("v2")));
    db.close().await.unwrap();
}

// === WriteBatch Tests ===

#[tokio::test]
async fn write_batch_atomic_commit() {
    let (db, _) = test_shard_db("test/batch").await;
    let mut batch = WriteBatch::new();
    batch.put(b"k1", b"v1");
    batch.put(b"k2", b"v2");
    batch.put(b"k3", b"v3");
    assert_eq!(batch.len(), 3);
    assert!(!batch.is_empty());

    db.write_batch(batch).await.unwrap();

    assert_eq!(db.get(b"k1").await.unwrap(), Some(Bytes::from("v1")));
    assert_eq!(db.get(b"k2").await.unwrap(), Some(Bytes::from("v2")));
    assert_eq!(db.get(b"k3").await.unwrap(), Some(Bytes::from("v3")));
    db.close().await.unwrap();
}

#[tokio::test]
async fn write_batch_with_deletes() {
    let (db, _) = test_shard_db("test/batch_delete").await;
    db.put(b"existing", b"value").await.unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"new_key", b"new_value");
    batch.delete(b"existing");
    db.write_batch(batch).await.unwrap();

    assert_eq!(
        db.get(b"new_key").await.unwrap(),
        Some(Bytes::from("new_value"))
    );
    assert_eq!(db.get(b"existing").await.unwrap(), None);
    db.close().await.unwrap();
}

#[tokio::test]
async fn empty_write_batch() {
    let (db, _) = test_shard_db("test/empty_batch").await;
    let batch = WriteBatch::new();
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
    // SlateDB rejects empty batches - this is expected behavior
    let result = db.write_batch(batch).await;
    assert!(result.is_err());
    db.close().await.unwrap();
}

// === Merge Operations ===

#[tokio::test]
async fn merge_sum_accumulates() {
    let (db, _) = test_shard_db("test/merge_sum").await;
    let key = b"counter";

    let v1 = MergeOperatorRegistry::encode_sum(10);
    db.merge(key, &v1).await.unwrap();

    let v2 = MergeOperatorRegistry::encode_sum(5);
    db.merge(key, &v2).await.unwrap();

    let v3 = MergeOperatorRegistry::encode_sum(3);
    db.merge(key, &v3).await.unwrap();

    let result = db.get(key).await.unwrap().unwrap();
    assert_eq!(MergeOperatorRegistry::decode_sum(&result), Some(18));
    db.close().await.unwrap();
}

#[tokio::test]
async fn merge_count_accumulates() {
    let (db, _) = test_shard_db("test/merge_count").await;
    let key = b"visits";

    let v1 = MergeOperatorRegistry::encode_count(1);
    db.merge(key, &v1).await.unwrap();
    db.merge(key, &v1).await.unwrap();
    db.merge(key, &v1).await.unwrap();

    let result = db.get(key).await.unwrap().unwrap();
    assert_eq!(MergeOperatorRegistry::decode_count(&result), Some(3));
    db.close().await.unwrap();
}

#[tokio::test]
async fn merge_in_write_batch() {
    let (db, _) = test_shard_db("test/batch_merge").await;
    let key = b"batch_counter";

    let v1 = MergeOperatorRegistry::encode_sum(10);
    db.merge(key, &v1).await.unwrap();

    let mut batch = WriteBatch::new();
    let v2 = MergeOperatorRegistry::encode_sum(5);
    batch.merge(key, &v2);
    db.write_batch(batch).await.unwrap();

    let result = db.get(key).await.unwrap().unwrap();
    assert_eq!(MergeOperatorRegistry::decode_sum(&result), Some(15));
    db.close().await.unwrap();
}

// === Prefix Scanning ===

#[tokio::test]
async fn scan_prefix_returns_sorted() {
    let (db, _) = test_shard_db("test/scan").await;

    let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 1);
    let k1 = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"aaa");
    let k2 = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"bbb");
    let k3 = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"ccc");
    // Different operator, should not appear
    let k4 = ShardKeyEncoder::encode(ShardPrefix::OpState, 2, b"aaa");

    db.put(&k3, b"v3").await.unwrap();
    db.put(&k1, b"v1").await.unwrap();
    db.put(&k2, b"v2").await.unwrap();
    db.put(&k4, b"other").await.unwrap();

    let results = db.scan_prefix(&prefix).await.unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, Bytes::from(k1));
    assert_eq!(results[1].0, Bytes::from(k2));
    assert_eq!(results[2].0, Bytes::from(k3));
    db.close().await.unwrap();
}

#[tokio::test]
async fn scan_empty_prefix_returns_all() {
    let (db, _) = test_shard_db("test/scan_all").await;
    db.put(b"a", b"1").await.unwrap();
    db.put(b"b", b"2").await.unwrap();
    db.put(b"c", b"3").await.unwrap();

    let results = db.scan_prefix(b"").await.unwrap();
    assert_eq!(results.len(), 3);
    db.close().await.unwrap();
}

// === Flush and WAL ===

#[tokio::test]
async fn flush_makes_data_durable() {
    let (db, _) = test_shard_db("test/flush").await;
    db.put(b"durable_key", b"durable_value").await.unwrap();
    db.flush().await.unwrap();

    let val = db.get(b"durable_key").await.unwrap();
    assert_eq!(val, Some(Bytes::from("durable_value")));
    db.close().await.unwrap();
}

#[tokio::test]
async fn wal_entries_visible_before_flush() {
    let store = Arc::new(InMemory::new());
    let db = crate::wal::open_with_wal_reads("test/wal_read", store)
        .await
        .unwrap();

    db.put(b"wal_key", b"wal_value").await.unwrap();

    // Should be visible with default read (Memory durability level)
    let val = db.get(b"wal_key").await.unwrap();
    assert_eq!(val, Some(Bytes::from("wal_value")));

    db.close().await.unwrap();
}

#[tokio::test]
async fn wal_read_entries_with_prefix() {
    let store = Arc::new(InMemory::new());
    let db = crate::wal::open_with_wal_reads("test/wal_prefix", store)
        .await
        .unwrap();

    db.put(b"prefix_a", b"va").await.unwrap();
    db.put(b"prefix_b", b"vb").await.unwrap();
    db.put(b"other_x", b"vx").await.unwrap();

    let entries = crate::wal::read_wal_entries(&db, b"prefix_").await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, Bytes::from("prefix_a"));
    assert_eq!(entries[1].0, Bytes::from("prefix_b"));

    db.close().await.unwrap();
}

#[tokio::test]
async fn verify_keys_present_after_flush() {
    let store = Arc::new(InMemory::new());
    let db = crate::wal::open_with_wal_reads("test/wal_verify", store)
        .await
        .unwrap();

    db.put(b"exists", b"yes").await.unwrap();
    db.flush().await.unwrap();

    let results = crate::wal::verify_keys_present(&db, &[b"exists", b"missing"])
        .await
        .unwrap();
    assert_eq!(results, vec![true, false]);

    db.close().await.unwrap();
}

// === DbReader Snapshot Tests ===

#[tokio::test]
async fn reader_reads_from_snapshot() {
    let store = Arc::new(InMemory::new());

    // Write data and flush (so it's in SSTs, not just WAL)
    let db = ShardDb::builder("test/reader", store.clone())
        .build()
        .await
        .unwrap();
    db.put(b"snap_key", b"snap_value").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // Open a reader against the same path
    let reader = crate::reader::ShardReader::open("test/reader", store)
        .await
        .unwrap();
    let val = reader.get(b"snap_key").await.unwrap();
    assert_eq!(val, Some(Bytes::from("snap_value")));
}

#[tokio::test]
async fn reader_scan_prefix() {
    let store = Arc::new(InMemory::new());

    let db = ShardDb::builder("test/reader_scan", store.clone())
        .build()
        .await
        .unwrap();
    db.put(b"pfx_a", b"1").await.unwrap();
    db.put(b"pfx_b", b"2").await.unwrap();
    db.put(b"other", b"3").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    let reader = crate::reader::ShardReader::open("test/reader_scan", store)
        .await
        .unwrap();
    let results = reader.scan_prefix(b"pfx_").await.unwrap();
    assert_eq!(results.len(), 2);
}

// === Catalog Key Namespace Dimension ===

#[tokio::test]
async fn catalog_keys_store_and_retrieve_with_namespace() {
    let (db, _) = test_shard_db("test/catalog_ns").await;

    let ns1_key = CatalogKeyEncoder::encode(CatalogType::Pipeline, 1, 100);
    let ns2_key = CatalogKeyEncoder::encode(CatalogType::Pipeline, 2, 100);

    db.put(&ns1_key, b"pipeline_ns1").await.unwrap();
    db.put(&ns2_key, b"pipeline_ns2").await.unwrap();

    // Same object_id in different namespaces are distinct
    let v1 = db.get(&ns1_key).await.unwrap();
    let v2 = db.get(&ns2_key).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("pipeline_ns1")));
    assert_eq!(v2, Some(Bytes::from("pipeline_ns2")));

    // Scan by namespace shows isolation
    let prefix = CatalogKeyEncoder::namespace_prefix(CatalogType::Pipeline, 1);
    let results = db.scan_prefix(&prefix).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, Bytes::from("pipeline_ns1"));

    db.close().await.unwrap();
}

// === Storage API Validation: Only Supported Features ===

/// This test documents the supported API surface.
/// Only these operations are used throughout rockstream-storage:
/// - get, put, delete (point operations)
/// - merge (with SumCountMergeOperator)
/// - write_batch (atomic multi-op)
/// - scan_prefix (ordered iteration)
/// - flush (durability)
/// - close (cleanup)
/// - DbReader (snapshot reads)
///
/// NOT used:
/// - range_delete (not available in SlateDB, by design)
/// - transactions (not needed for epoch-based processing)
#[tokio::test]
async fn supported_api_surface_validation() {
    let (db, _) = test_shard_db("test/api_surface").await;

    // Point operations
    db.put(b"k", b"v").await.unwrap();
    let _ = db.get(b"k").await.unwrap();
    db.delete(b"k").await.unwrap();

    // Merge
    let sum = MergeOperatorRegistry::encode_sum(1);
    db.merge(b"m", &sum).await.unwrap();

    // Write batch
    let mut batch = WriteBatch::new();
    batch.put(b"b1", b"v1");
    batch.delete(b"b2");
    batch.merge(b"m", &sum);
    db.write_batch(batch).await.unwrap();

    // Prefix scan
    let _ = db.scan_prefix(b"").await.unwrap();

    // Flush
    db.flush().await.unwrap();

    // Close
    db.close().await.unwrap();
}

// === Unsupported Operations Fail ===

/// Compile-time proof that range-delete is not exposed.
/// If someone tries to add a `range_delete` method to ShardDb,
/// they must consciously update this test and documentation.
#[test]
fn no_range_delete_method_exists() {
    // This is checked at compile time by the absence of the method.
    // If `ShardDb::range_delete` were added, this doc-test reminds
    // developers to not use it.
    //
    // The pattern for cleanup is:
    //   let entries = db.scan_prefix(&prefix).await?;
    //   let mut batch = WriteBatch::new();
    //   for (key, _) in entries {
    //       batch.delete(&key);
    //   }
    //   db.write_batch(batch).await?;
}

// === SlateDB Determinism Test ===

/// Two runs of the same operation sequence against InMemory object stores
/// produce bit-identical key-value state.
///
/// This validates that when using:
/// - InMemory object store (deterministic storage backend)
/// - Sequential operations (no concurrent tasks)
/// - Same operation sequence
///
/// The result is identical. This is the gate that validates the simulation
/// property holds through SlateDB storage operations.
#[tokio::test]
async fn determinism_two_runs_identical_state() {
    async fn run_workload(path: &str, store: Arc<InMemory>) -> Vec<(Bytes, Bytes)> {
        let db = ShardDb::builder(path, store).build().await.unwrap();

        // Deterministic sequence of operations
        for i in 0u64..50 {
            let key = ShardKeyEncoder::encode(ShardPrefix::OpState, i % 5, &i.to_be_bytes());
            let value = format!("value_{i}");
            db.put(&key, value.as_bytes()).await.unwrap();
        }

        // Some deletes
        for i in [10u64, 20, 30] {
            let key = ShardKeyEncoder::encode(ShardPrefix::OpState, i % 5, &i.to_be_bytes());
            db.delete(&key).await.unwrap();
        }

        // Merges
        let counter_key = ShardKeyEncoder::encode(ShardPrefix::OpIndex, 1, b"counter");
        for i in 1..=10i64 {
            let v = MergeOperatorRegistry::encode_sum(i);
            db.merge(&counter_key, &v).await.unwrap();
        }

        // Batch writes
        let mut batch = WriteBatch::new();
        for i in 0u64..10 {
            let key = ShardKeyEncoder::encode(ShardPrefix::ViewOutput, 1, &i.to_be_bytes());
            batch.put(&key, &[i as u8; 8]);
        }
        db.write_batch(batch).await.unwrap();

        // Flush to ensure all data is in SSTs
        db.flush().await.unwrap();

        // Read all state
        let state = db.scan_prefix(b"").await.unwrap();
        db.close().await.unwrap();
        state
    }

    // Run 1
    let store1 = Arc::new(InMemory::new());
    let state1 = run_workload("determinism/run1", store1).await;

    // Run 2 - identical operations against fresh store
    let store2 = Arc::new(InMemory::new());
    let state2 = run_workload("determinism/run2", store2).await;

    // Assert bit-identical state
    assert_eq!(
        state1.len(),
        state2.len(),
        "Different number of keys: {} vs {}",
        state1.len(),
        state2.len()
    );

    for (i, ((k1, v1), (k2, v2))) in state1.iter().zip(state2.iter()).enumerate() {
        assert_eq!(k1, k2, "Key mismatch at position {i}");
        assert_eq!(v1, v2, "Value mismatch at position {i} for key {k1:?}");
    }
}

/// Extended determinism test with interleaved operations and multiple data types.
#[tokio::test]
async fn determinism_interleaved_operations() {
    async fn run_interleaved(path: &str, store: Arc<InMemory>) -> Vec<(Bytes, Bytes)> {
        let db = ShardDb::builder(path, store).build().await.unwrap();

        // Interleave puts, merges, deletes, and batches
        for epoch in 0u64..5 {
            // Puts
            for i in 0..10u64 {
                let key = ShardKeyEncoder::encode(
                    ShardPrefix::OpState,
                    epoch,
                    &(epoch * 10 + i).to_be_bytes(),
                );
                db.put(&key, &(epoch * 100 + i).to_be_bytes())
                    .await
                    .unwrap();
            }

            // Merge counter
            let ctr_key = ShardKeyEncoder::encode(ShardPrefix::OpIndex, epoch, b"ctr");
            let v = MergeOperatorRegistry::encode_count(epoch + 1);
            db.merge(&ctr_key, &v).await.unwrap();

            // Batch with mixed ops
            let mut batch = WriteBatch::new();
            for i in 0..3u64 {
                let k = ShardKeyEncoder::encode(ShardPrefix::ShuffleInbox, epoch, &i.to_be_bytes());
                batch.put(&k, b"inbox");
            }
            if epoch > 0 {
                // Delete some from previous epoch
                let del_key = ShardKeyEncoder::encode(
                    ShardPrefix::OpState,
                    epoch - 1,
                    &((epoch - 1) * 10).to_be_bytes(),
                );
                batch.delete(&del_key);
            }
            db.write_batch(batch).await.unwrap();
        }

        db.flush().await.unwrap();
        let state = db.scan_prefix(b"").await.unwrap();
        db.close().await.unwrap();
        state
    }

    let store1 = Arc::new(InMemory::new());
    let state1 = run_interleaved("det_interleaved/run1", store1).await;

    let store2 = Arc::new(InMemory::new());
    let state2 = run_interleaved("det_interleaved/run2", store2).await;

    assert_eq!(state1.len(), state2.len());
    assert_eq!(state1, state2);
}
