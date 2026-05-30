//! Arrow IPC serialization helpers for the exchange wire format.
//!
//! Exchange batches are serialized to the Arrow IPC stream format (not the
//! file format).  The IPC stream format is self-describing and allows a
//! sequence of `RecordBatch`es to be framed into a single byte buffer that
//! can be written to a channel, a gRPC stream, or a durable shuffle object.
//!
//! ## Wire layout
//!
//! A serialized batch is:
//! 1. IPC continuation token + schema message (written once per stream).
//! 2. One or more `RecordBatch` IPC messages.
//! 3. IPC EOS marker.
//!
//! Because `encode_batch` and `decode_batch` operate on a single batch at a
//! time, each call produces a complete self-contained IPC stream (schema +
//! data + EOS).  This simplifies random-access reads from durable shuffle
//! objects.

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::record_batch::RecordBatch;
use std::io::Cursor;

/// Serialize a single `RecordBatch` to Arrow IPC stream bytes.
///
/// The returned `Vec<u8>` is a complete, self-contained IPC stream that
/// includes the schema message and a trailing EOS marker.
///
/// # Errors
///
/// Returns an `arrow::error::ArrowError` if serialization fails (e.g. the
/// batch contains an unsupported data type).
pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buf, batch.schema().as_ref(), options)?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

/// Deserialize Arrow IPC stream bytes back into a `RecordBatch`.
///
/// Expects the bytes to have been produced by [`encode_batch`]: a complete
/// IPC stream with exactly one data batch.
///
/// # Errors
///
/// Returns an error if the bytes are malformed or the stream contains no
/// batches.
pub fn decode_batch(bytes: &[u8]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;
    reader.next().ok_or_else(|| {
        arrow::error::ArrowError::ParseError("IPC stream contained no batches".to_owned())
    })?
}

/// Returns the number of bytes that would be used to encode `batch`.
///
/// Useful for pre-sizing buffers and computing the `bytes_avoided` metric.
pub fn encoded_size(batch: &RecordBatch) -> usize {
    // The cheapest estimate is to encode and measure; for large batches the
    // caller may prefer to use a placeholder but this is exact.
    encode_batch(batch).map(|v| v.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let array = Arc::new(Int64Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[test]
    fn roundtrip_single_batch() {
        let original = make_batch(&[1, 2, 3]);
        let bytes = encode_batch(&original).expect("encode ok");
        let decoded = decode_batch(&bytes).expect("decode ok");
        assert_eq!(decoded.num_rows(), 3);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(2), 3);
    }

    #[test]
    fn roundtrip_empty_batch() {
        let original = make_batch(&[]);
        let bytes = encode_batch(&original).expect("encode ok");
        let decoded = decode_batch(&bytes).expect("decode ok");
        assert_eq!(decoded.num_rows(), 0);
    }

    #[test]
    fn encoded_size_nonzero_for_nonempty_batch() {
        let batch = make_batch(&[42, 99]);
        let size = encoded_size(&batch);
        assert!(size > 0, "encoded size should be non-zero");
    }

    #[test]
    fn decode_empty_bytes_returns_error() {
        let result = decode_batch(&[]);
        assert!(result.is_err(), "empty bytes should fail to decode");
    }
}
