//! Arrow batch utilities implementing the `_weight` column convention.
//!
//! Every ZSet delta that is represented as an Arrow `RecordBatch` carries a
//! special `_weight` column (Int64, non-nullable) as its **last** column.
//! Positive weights are insertions, negative are deletions.
//!
//! # `_weight` convention
//!
//! | Column      | Type    | Meaning                                       |
//! |-------------|---------|-----------------------------------------------|
//! | user cols…  | any     | Row identity and payload                      |
//! | `_weight`   | Int64   | IVM delta weight (+1 = insert, -1 = delete)   |
//!
//! The `_weight` column is always the **last** column in a weighted batch.
//! Operators that are weight-aware strip it before evaluating predicates or
//! projections, then re-attach it to the output.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

/// The reserved column name for IVM delta weights.
pub const WEIGHT_COLUMN: &str = "_weight";

/// Returns the Arrow field definition for the `_weight` column.
pub fn weight_field() -> Field {
    Field::new(WEIGHT_COLUMN, DataType::Int64, false)
}

/// Build a `SchemaRef` that appends `_weight` as the last field.
pub fn weighted_schema(data_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = data_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(weight_field());
    Arc::new(Schema::new(fields))
}

/// Append a `_weight` column to an existing `RecordBatch`.
///
/// The `weights` slice must have the same length as the batch.
pub fn append_weight_column(
    batch: RecordBatch,
    weights: &[i64],
) -> Result<RecordBatch, ArrowError> {
    if batch.num_rows() != weights.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "weights length {} does not match batch row count {}",
            weights.len(),
            batch.num_rows()
        )));
    }

    let new_schema = weighted_schema(&batch.schema());
    let weight_array: ArrayRef = Arc::new(Int64Array::from(weights.to_vec()));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(weight_array);

    RecordBatch::try_new(new_schema, columns)
}

/// Extract the `_weight` column from a weighted batch.
///
/// Returns `None` if the batch has no `_weight` column.
pub fn extract_weight_array(batch: &RecordBatch) -> Option<&Int64Array> {
    let idx = batch.schema().index_of(WEIGHT_COLUMN).ok()?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
}

/// Split a weighted batch into (data batch, weights).
///
/// The `_weight` column is removed from the returned data batch.
/// Returns `None` if the batch has no `_weight` column.
pub fn split_weight_column(
    batch: &RecordBatch,
) -> Option<(RecordBatch, Vec<i64>)> {
    let idx = batch.schema().index_of(WEIGHT_COLUMN).ok()?;

    let weights: Vec<i64> = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()?
        .iter()
        .map(|v| v.unwrap_or(0))
        .collect();

    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != idx)
        .map(|(_, f)| f.as_ref().clone())
        .collect();

    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != idx)
        .map(|(_, c)| c.clone())
        .collect();

    let schema = Arc::new(Schema::new(fields));
    let data_batch = RecordBatch::try_new(schema, columns).ok()?;
    Some((data_batch, weights))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_data_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
        RecordBatch::try_new(schema, vec![ids, vals]).unwrap()
    }

    #[test]
    fn weight_field_has_correct_type() {
        let f = weight_field();
        assert_eq!(f.name(), WEIGHT_COLUMN);
        assert_eq!(f.data_type(), &DataType::Int64);
        assert!(!f.is_nullable());
    }

    #[test]
    fn append_and_split_roundtrip() {
        let data = make_data_batch();
        let weights = vec![1i64, -1, 2];

        let weighted = append_weight_column(data.clone(), &weights).unwrap();
        assert_eq!(weighted.num_columns(), 3);
        assert_eq!(weighted.schema().field(2).name(), WEIGHT_COLUMN);

        let (recovered, recovered_weights) = split_weight_column(&weighted).unwrap();
        assert_eq!(recovered.num_columns(), 2);
        assert_eq!(recovered_weights, weights);
    }

    #[test]
    fn extract_weight_array_finds_column() {
        let data = make_data_batch();
        let weights = vec![1i64, -1, 2];
        let weighted = append_weight_column(data, &weights).unwrap();

        let arr = extract_weight_array(&weighted).unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), -1);
        assert_eq!(arr.value(2), 2);
    }

    #[test]
    fn append_wrong_length_fails() {
        let data = make_data_batch();
        assert!(append_weight_column(data, &[1i64, 2]).is_err());
    }
}
