//! Row codec abstraction: bridges `ZSetRow` byte representation and Arrow
//! `RecordBatch` for DataFusion expression evaluation.
//!
//! A `RowCodec` implementation knows how to:
//! - Encode a slice of `ZSetRow`s into a typed Arrow `RecordBatch`
//! - Decode a single-row `RecordBatch` back to `(key, value)` bytes
//!
//! This is the integration boundary between the byte-oriented ZSet layer
//! and the Arrow/DataFusion typed expression layer introduced in v0.6.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use rockstream_types::batch::ZSetRow;

/// Bridge between raw `ZSetRow` bytes and Arrow `RecordBatch`.
///
/// Implementations define the row encoding and must be `Send + Sync` so they
/// can be shared across operator threads.
pub trait RowCodec: Send + Sync + 'static {
    /// The Arrow schema for the data columns (without `_weight`).
    fn schema(&self) -> SchemaRef;

    /// Encode a slice of rows into a `RecordBatch`.
    ///
    /// The resulting batch has columns matching `self.schema()`. Row ordering
    /// matches the input slice order.
    ///
    /// # Panics
    /// May panic on malformed row bytes; callers are responsible for ensuring
    /// that rows were encoded by the same codec.
    fn encode_batch(&self, rows: &[ZSetRow]) -> RecordBatch;

    /// Decode a single-row `RecordBatch` back to `(key_bytes, value_bytes)`.
    ///
    /// Returns `None` if the batch is empty or decoding fails.
    fn decode_batch_single(&self, batch: &RecordBatch) -> Option<(Vec<u8>, Vec<u8>)>;
}

/// A codec for two-column `{id: Int64, val: Int64}` rows.
///
/// Encoding:
/// - `key`   → 8-byte big-endian `id` (Int64)
/// - `value` → 8-byte big-endian `val` (Int64)
///
/// This codec is used by the property-test suite to create typed rows for
/// DataFusion expression evaluation comparisons.
pub struct Int64RowCodec {
    schema: SchemaRef,
}

impl Int64RowCodec {
    /// Create a new codec with schema `{id: Int64, val: Int64}`.
    pub fn new() -> Arc<Self> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("val", arrow::datatypes::DataType::Int64, false),
        ]));
        Arc::new(Self { schema })
    }

    /// Encode `(id, val)` as `(key_bytes, value_bytes)`.
    pub fn encode_row(id: i64, val: i64) -> (Vec<u8>, Vec<u8>) {
        (id.to_be_bytes().to_vec(), val.to_be_bytes().to_vec())
    }

    /// Decode `(key_bytes, value_bytes)` to `(id, val)`.
    ///
    /// Returns `(0, 0)` if the slices are too short.
    pub fn decode_row(key: &[u8], value: &[u8]) -> (i64, i64) {
        let id = if key.len() >= 8 {
            i64::from_be_bytes(key[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        let val = if value.len() >= 8 {
            i64::from_be_bytes(value[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        (id, val)
    }
}

impl Default for Int64RowCodec {
    fn default() -> Self {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("val", arrow::datatypes::DataType::Int64, false),
        ]));
        Self { schema }
    }
}

impl RowCodec for Int64RowCodec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn encode_batch(&self, rows: &[ZSetRow]) -> RecordBatch {
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| {
                if r.key.len() >= 8 {
                    i64::from_be_bytes(r.key[..8].try_into().unwrap_or([0u8; 8]))
                } else {
                    0
                }
            })
            .collect();

        let vals: Vec<i64> = rows
            .iter()
            .map(|r| {
                if r.value.len() >= 8 {
                    i64::from_be_bytes(r.value[..8].try_into().unwrap_or([0u8; 8]))
                } else {
                    0
                }
            })
            .collect();

        let id_array: arrow::array::ArrayRef =
            std::sync::Arc::new(arrow::array::Int64Array::from(ids));
        let val_array: arrow::array::ArrayRef =
            std::sync::Arc::new(arrow::array::Int64Array::from(vals));

        RecordBatch::try_new(self.schema.clone(), vec![id_array, val_array])
            .expect("Int64RowCodec encode_batch failed")
    }

    fn decode_batch_single(&self, batch: &RecordBatch) -> Option<(Vec<u8>, Vec<u8>)> {
        if batch.num_rows() == 0 {
            return None;
        }
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()?
            .value(0);
        let val = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()?
            .value(0);
        Some((id.to_be_bytes().to_vec(), val.to_be_bytes().to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let codec = Int64RowCodec::new();
        let (key, value) = Int64RowCodec::encode_row(42, 99);
        let row = ZSetRow {
            key: key.clone(),
            value: value.clone(),
            weight: 1,
        };
        let batch = codec.encode_batch(&[row]);
        assert_eq!(batch.num_rows(), 1);

        let (decoded_key, decoded_val) = codec.decode_batch_single(&batch).unwrap();
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_val, value);
    }

    #[test]
    fn decode_raw_bytes() {
        let (k, v) = Int64RowCodec::encode_row(-1, 42);
        let (id, val) = Int64RowCodec::decode_row(&k, &v);
        assert_eq!(id, -1);
        assert_eq!(val, 42);
    }
}
