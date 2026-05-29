//! Schema compatibility rules.
//!
//! Implements the "compatible change" rules for v0.12. A change is compatible
//! if existing consumers (views, queries, connectors) can continue to work
//! without modification. An incompatible change requires a view rebuild or a
//! new catalog entry, and returns `RS-1002`.
//!
//! Compatible changes:
//! - Adding a **nullable** column at the end.
//! - Widening a numeric type (e.g., Int32 → Int64).
//! - Adding a column with a DEFAULT expression (not yet implemented; reserved).
//!
//! Incompatible changes (→ RS-1002):
//! - Removing any column.
//! - Renaming any column.
//! - Changing a column's data type to a narrower or incompatible type.
//! - Making a nullable column non-nullable.
//! - Adding a **non-nullable** column without a default.
//! - Reordering columns.

use crate::error::CatalogError;
use crate::schema::{DataType, SchemaVersion};

/// Result of a compatibility check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatibilityResult {
    /// The change is backward-compatible; existing consumers continue to work.
    Compatible,
    /// The change is incompatible.
    Incompatible { reason: String },
}

impl CompatibilityResult {
    /// Returns `Ok(())` for compatible, `Err(RS-1002)` for incompatible.
    pub fn into_result(self) -> Result<(), CatalogError> {
        match self {
            Self::Compatible => Ok(()),
            Self::Incompatible { reason } => Err(CatalogError::IncompatibleSchemaChange { reason }),
        }
    }

    /// Returns `true` if the change is compatible.
    pub fn is_compatible(&self) -> bool {
        matches!(self, Self::Compatible)
    }
}

/// Check whether `new_schema` is a compatible evolution of `old_schema`.
///
/// Returns `Compatible` or `Incompatible { reason }`.
pub fn check_schema_change(old: &SchemaVersion, new: &SchemaVersion) -> CompatibilityResult {
    // All existing columns must be present, in the same order, at the front.
    let old_count = old.columns.len();

    if new.columns.len() < old_count {
        return CompatibilityResult::Incompatible {
            reason: format!(
                "new schema has {} columns, old has {}; columns may not be removed",
                new.columns.len(),
                old_count
            ),
        };
    }

    // Check that the first `old_count` columns match exactly (name, type,
    // nullability direction — widening is OK).
    for (idx, (old_col, new_col)) in old.columns.iter().zip(new.columns.iter()).enumerate() {
        if old_col.name != new_col.name {
            return CompatibilityResult::Incompatible {
                reason: format!(
                    "column at index {idx} renamed from '{}' to '{}'; renames are incompatible",
                    old_col.name, new_col.name
                ),
            };
        }

        if !is_type_widening(old_col.data_type, new_col.data_type) {
            return CompatibilityResult::Incompatible {
                reason: format!(
                    "column '{}': type change from {} to {} is incompatible",
                    old_col.name, old_col.data_type, new_col.data_type
                ),
            };
        }

        // Cannot make a nullable column non-nullable (would break NULLs in storage).
        if old_col.nullable && !new_col.nullable {
            return CompatibilityResult::Incompatible {
                reason: format!(
                    "column '{}': cannot change nullable→non-nullable; existing NULL values would be invalid",
                    old_col.name
                ),
            };
        }
    }

    // Any added columns must be nullable (no DEFAULT support yet).
    for new_col in &new.columns[old_count..] {
        if !new_col.nullable {
            return CompatibilityResult::Incompatible {
                reason: format!(
                    "added column '{}' is non-nullable; new columns must be nullable (no DEFAULT support)",
                    new_col.name
                ),
            };
        }
    }

    CompatibilityResult::Compatible
}

/// Returns true if `new_type` is the same as or a lossless widening of `old_type`.
///
/// Widening pairs: Int32 → Int64, Float32 → Float64.
/// All other type changes are incompatible.
fn is_type_widening(old: DataType, new: DataType) -> bool {
    if old == new {
        return true;
    }
    matches!(
        (old, new),
        (DataType::Int32, DataType::Int64) | (DataType::Float32, DataType::Float64)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, DataType, SchemaVersion};

    fn base_schema() -> SchemaVersion {
        SchemaVersion::new(vec![
            ColumnDef::required("id", DataType::Int64),
            ColumnDef::required("amount", DataType::Float64),
        ])
    }

    #[test]
    fn compatible_add_nullable_column() {
        let old = base_schema();
        let mut new_cols = old.columns.clone();
        new_cols.push(ColumnDef::nullable("notes", DataType::Utf8));
        let new = SchemaVersion {
            version: 2,
            columns: new_cols,
        };
        assert_eq!(
            check_schema_change(&old, &new),
            CompatibilityResult::Compatible
        );
    }

    #[test]
    fn compatible_widen_int32_to_int64() {
        let old = SchemaVersion::new(vec![ColumnDef::required("x", DataType::Int32)]);
        let new = SchemaVersion {
            version: 2,
            columns: vec![ColumnDef::required("x", DataType::Int64)],
        };
        assert_eq!(
            check_schema_change(&old, &new),
            CompatibilityResult::Compatible
        );
    }

    #[test]
    fn compatible_widen_float32_to_float64() {
        let old = SchemaVersion::new(vec![ColumnDef::required("v", DataType::Float32)]);
        let new = SchemaVersion {
            version: 2,
            columns: vec![ColumnDef::required("v", DataType::Float64)],
        };
        assert_eq!(
            check_schema_change(&old, &new),
            CompatibilityResult::Compatible
        );
    }

    #[test]
    fn compatible_unchanged_schema() {
        let old = base_schema();
        let new = base_schema();
        assert_eq!(
            check_schema_change(&old, &new),
            CompatibilityResult::Compatible
        );
    }

    #[test]
    fn incompatible_column_removed_returns_rs_1002() {
        let old = base_schema();
        let new = SchemaVersion::new(vec![ColumnDef::required("id", DataType::Int64)]);
        let result = check_schema_change(&old, &new);
        assert!(matches!(result, CompatibilityResult::Incompatible { .. }));
        assert!(result.into_result().is_err());
    }

    #[test]
    fn incompatible_column_removed_error_code() {
        let old = base_schema();
        let new = SchemaVersion::new(vec![ColumnDef::required("id", DataType::Int64)]);
        let err = check_schema_change(&old, &new).into_result().unwrap_err();
        assert_eq!(err.error_code(), rockstream_types::error_code::RS_1002);
    }

    #[test]
    fn incompatible_column_renamed() {
        let old = base_schema();
        let new = SchemaVersion::new(vec![
            ColumnDef::required("identifier", DataType::Int64), // renamed
            ColumnDef::required("amount", DataType::Float64),
        ]);
        let result = check_schema_change(&old, &new);
        assert!(matches!(result, CompatibilityResult::Incompatible { .. }));
    }

    #[test]
    fn incompatible_type_change_int64_to_utf8() {
        let old = base_schema();
        let new = SchemaVersion::new(vec![
            ColumnDef::required("id", DataType::Utf8), // type changed
            ColumnDef::required("amount", DataType::Float64),
        ]);
        let result = check_schema_change(&old, &new);
        assert!(matches!(result, CompatibilityResult::Incompatible { .. }));
    }

    #[test]
    fn incompatible_add_non_nullable_column() {
        let old = base_schema();
        let mut new_cols = old.columns.clone();
        new_cols.push(ColumnDef::required("required_new", DataType::Int64)); // non-nullable
        let new = SchemaVersion {
            version: 2,
            columns: new_cols,
        };
        let result = check_schema_change(&old, &new);
        assert!(matches!(result, CompatibilityResult::Incompatible { .. }));
    }

    #[test]
    fn incompatible_nullable_to_non_nullable() {
        let old = SchemaVersion::new(vec![ColumnDef::nullable("tag", DataType::Utf8)]);
        let new = SchemaVersion::new(vec![ColumnDef::required("tag", DataType::Utf8)]);
        let result = check_schema_change(&old, &new);
        assert!(matches!(result, CompatibilityResult::Incompatible { .. }));
    }
}
