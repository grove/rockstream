//! Plan codec: Substrait-extension JSON encoding for `PlanNode`.
//!
//! Serializes a `PlanNode` to a JSON wire format tagged as
//! `substrait-ext/rockstream/v1`. Each operator node that uses a merge law
//! carries `"law_id"` and `"law_version"` fields. On decoding, every such
//! field is checked against the provided `LawRegistry`; an unknown law returns
//! `RS-5002`.
//!
//! # Wire format sketch
//!
//! ```json
//! {
//!   "format": "substrait-ext/rockstream/v1",
//!   "schema_version": 1,
//!   "plan": { ... }
//! }
//! ```
//!
//! Each plan node is represented as a JSON object with a `"type"` discriminant.
//! Aggregate nodes additionally carry `"law_id"`, `"law_version"`, and
//! optionally `"not_merge_safe_reason"`.

use crate::error::CatalogError;
use rockstream_plan::{AggregateExpr, AggregateFunc, BinaryOp, Expr, PlanNode};
use rockstream_types::laws::registry::LawRegistry;
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The wire-format version tag.
const FORMAT: &str = "substrait-ext/rockstream/v1";
/// The current schema version of the codec format itself.
const CODEC_SCHEMA_VERSION: u32 = 1;

// ─── Wire types ──────────────────────────────────────────────────────────────

/// Top-level persisted plan envelope.
#[derive(Debug, Serialize, Deserialize)]
struct PlanEnvelope {
    format: String,
    schema_version: u32,
    plan: Value,
}

/// Merge law annotation embedded in an operator node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LawAnnotation {
    pub law_id: u16,
    pub law_version: u16,
}

impl LawAnnotation {
    pub fn new(id: MergeLawId, version: MergeLawVersion) -> Self {
        Self {
            law_id: id.0,
            law_version: version.0,
        }
    }
}

// ─── Public API ──────────────────────────────────────────────────────────────

/// Encode a `PlanNode` with law annotations to bytes.
///
/// The `law_for` callback is called for each `Aggregate` and `Union` node and
/// should return the `(MergeLawId, MergeLawVersion)` for that node's arrangement,
/// or `None` if the node is not merge-safe (in which case the node's
/// `not_merge_safe_reason` string is embedded instead).
pub fn encode(
    plan: &PlanNode,
    law_for: &dyn Fn(&PlanNode) -> Option<(MergeLawId, MergeLawVersion)>,
) -> Result<Vec<u8>, CatalogError> {
    let plan_value = encode_node(plan, law_for, "root");
    let envelope = PlanEnvelope {
        format: FORMAT.to_owned(),
        schema_version: CODEC_SCHEMA_VERSION,
        plan: plan_value,
    };
    serde_json::to_vec(&envelope)
        .map_err(|e| CatalogError::Codec(format!("serialization failed: {e}")))
}

/// Decode a `PlanNode` from bytes, checking every law annotation against
/// the provided registry.
///
/// Returns `RS-5002` if any law in the encoded plan is not registered.
pub fn decode(bytes: &[u8], registry: &LawRegistry) -> Result<PlanNode, CatalogError> {
    let envelope: PlanEnvelope = serde_json::from_slice(bytes)
        .map_err(|e| CatalogError::Codec(format!("deserialization failed: {e}")))?;

    if envelope.format != FORMAT {
        return Err(CatalogError::Codec(format!(
            "unknown plan format '{}'; expected '{FORMAT}'",
            envelope.format
        )));
    }

    decode_node(&envelope.plan, registry, "root")
}

// ─── Encoding helpers ─────────────────────────────────────────────────────────

fn encode_node(
    plan: &PlanNode,
    law_for: &dyn Fn(&PlanNode) -> Option<(MergeLawId, MergeLawVersion)>,
    path: &str,
) -> Value {
    match plan {
        PlanNode::Source { name } => {
            serde_json::json!({ "type": "Source", "name": name })
        }
        PlanNode::Filter { input, predicate } => {
            serde_json::json!({
                "type": "Filter",
                "predicate": encode_expr(predicate),
                "input": encode_node(input, law_for, &format!("{path}/filter")),
            })
        }
        PlanNode::Project { input, columns } => {
            serde_json::json!({
                "type": "Project",
                "columns": columns.iter().map(encode_expr).collect::<Vec<_>>(),
                "input": encode_node(input, law_for, &format!("{path}/project")),
            })
        }
        PlanNode::Map { input, func } => {
            serde_json::json!({
                "type": "Map",
                "func": encode_expr(func),
                "input": encode_node(input, law_for, &format!("{path}/map")),
            })
        }
        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let mut obj = serde_json::json!({
                "type": "Aggregate",
                "group_by": group_by.iter().map(encode_expr).collect::<Vec<_>>(),
                "aggregates": aggregates.iter().map(encode_agg_expr).collect::<Vec<_>>(),
                "input": encode_node(input, law_for, &format!("{path}/agg_input")),
            });
            if let Some((law_id, law_ver)) = law_for(plan) {
                obj["law_id"] = serde_json::json!(law_id.0);
                obj["law_version"] = serde_json::json!(law_ver.0);
            }
            obj
        }
        PlanNode::Union { left, right } => {
            let mut obj = serde_json::json!({
                "type": "Union",
                "left": encode_node(left, law_for, &format!("{path}/union_left")),
                "right": encode_node(right, law_for, &format!("{path}/union_right")),
            });
            if let Some((law_id, law_ver)) = law_for(plan) {
                obj["law_id"] = serde_json::json!(law_id.0);
                obj["law_version"] = serde_json::json!(law_ver.0);
            }
            obj
        }
        PlanNode::Join {
            left,
            right,
            condition,
        } => {
            serde_json::json!({
                "type": "Join",
                "condition": encode_expr(condition),
                "left": encode_node(left, law_for, &format!("{path}/join_left")),
                "right": encode_node(right, law_for, &format!("{path}/join_right")),
            })
        }
    }
}

fn encode_expr(expr: &Expr) -> Value {
    match expr {
        Expr::Column(idx) => serde_json::json!({ "type": "Column", "index": idx }),
        Expr::Literal(bytes) => serde_json::json!({ "type": "Literal", "bytes": bytes }),
        Expr::BinaryOp { op, left, right } => serde_json::json!({
            "type": "BinaryOp",
            "op": encode_binary_op(*op),
            "left": encode_expr(left),
            "right": encode_expr(right),
        }),
    }
}

fn encode_binary_op(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "Eq",
        BinaryOp::Ne => "Ne",
        BinaryOp::Lt => "Lt",
        BinaryOp::Le => "Le",
        BinaryOp::Gt => "Gt",
        BinaryOp::Ge => "Ge",
        BinaryOp::Add => "Add",
        BinaryOp::Sub => "Sub",
        BinaryOp::Mul => "Mul",
        BinaryOp::Div => "Div",
        BinaryOp::And => "And",
        BinaryOp::Or => "Or",
    }
}

fn encode_agg_expr(agg: &AggregateExpr) -> Value {
    serde_json::json!({
        "func": encode_agg_func(agg.func),
        "input": encode_expr(&agg.input),
        "distinct": agg.distinct,
    })
}

fn encode_agg_func(func: AggregateFunc) -> &'static str {
    match func {
        AggregateFunc::Count => "Count",
        AggregateFunc::Sum => "Sum",
        AggregateFunc::Avg => "Avg",
        AggregateFunc::Min => "Min",
        AggregateFunc::Max => "Max",
    }
}

// ─── Decoding helpers ─────────────────────────────────────────────────────────

fn decode_node(v: &Value, registry: &LawRegistry, path: &str) -> Result<PlanNode, CatalogError> {
    let ty = v["type"]
        .as_str()
        .ok_or_else(|| CatalogError::Codec(format!("{path}: missing 'type' field")))?;

    match ty {
        "Source" => {
            let name = v["name"]
                .as_str()
                .ok_or_else(|| CatalogError::Codec(format!("{path}: Source missing 'name'")))?
                .to_owned();
            Ok(PlanNode::Source { name })
        }
        "Filter" => {
            let predicate = decode_expr(&v["predicate"], path)?;
            let input = decode_node(&v["input"], registry, &format!("{path}/filter"))?;
            Ok(PlanNode::Filter {
                input: Box::new(input),
                predicate,
            })
        }
        "Project" => {
            let columns = v["columns"]
                .as_array()
                .ok_or_else(|| CatalogError::Codec(format!("{path}: Project missing 'columns'")))?
                .iter()
                .map(|e| decode_expr(e, path))
                .collect::<Result<Vec<_>, _>>()?;
            let input = decode_node(&v["input"], registry, &format!("{path}/project"))?;
            Ok(PlanNode::Project {
                input: Box::new(input),
                columns,
            })
        }
        "Map" => {
            let func = decode_expr(&v["func"], path)?;
            let input = decode_node(&v["input"], registry, &format!("{path}/map"))?;
            Ok(PlanNode::Map {
                input: Box::new(input),
                func,
            })
        }
        "Aggregate" => {
            // If law_id is present, validate it against the registry.
            if let Some(law_id_val) = v.get("law_id") {
                let law_id = law_id_val.as_u64().ok_or_else(|| {
                    CatalogError::Codec(format!("{path}: Aggregate 'law_id' not u64"))
                })? as u16;
                let law_version = v["law_version"].as_u64().ok_or_else(|| {
                    CatalogError::Codec(format!("{path}: Aggregate missing 'law_version'"))
                })? as u16;
                let mid = MergeLawId(law_id);
                if !registry.contains(mid) {
                    return Err(CatalogError::UnknownMergeLaw {
                        law_id,
                        law_version,
                        operator_path: path.to_owned(),
                    });
                }
            }

            let group_by = v["group_by"]
                .as_array()
                .ok_or_else(|| {
                    CatalogError::Codec(format!("{path}: Aggregate missing 'group_by'"))
                })?
                .iter()
                .map(|e| decode_expr(e, path))
                .collect::<Result<Vec<_>, _>>()?;
            let aggregates = v["aggregates"]
                .as_array()
                .ok_or_else(|| {
                    CatalogError::Codec(format!("{path}: Aggregate missing 'aggregates'"))
                })?
                .iter()
                .map(|e| decode_agg_expr(e, path))
                .collect::<Result<Vec<_>, _>>()?;
            let input = decode_node(&v["input"], registry, &format!("{path}/agg_input"))?;
            Ok(PlanNode::Aggregate {
                input: Box::new(input),
                group_by,
                aggregates,
            })
        }
        "Union" => {
            // If law_id is present, validate it against the registry.
            if let Some(law_id_val) = v.get("law_id") {
                let law_id = law_id_val
                    .as_u64()
                    .ok_or_else(|| CatalogError::Codec(format!("{path}: Union 'law_id' not u64")))?
                    as u16;
                let law_version = v["law_version"].as_u64().ok_or_else(|| {
                    CatalogError::Codec(format!("{path}: Union missing 'law_version'"))
                })? as u16;
                let mid = MergeLawId(law_id);
                if !registry.contains(mid) {
                    return Err(CatalogError::UnknownMergeLaw {
                        law_id,
                        law_version,
                        operator_path: path.to_owned(),
                    });
                }
            }

            let left = decode_node(&v["left"], registry, &format!("{path}/union_left"))?;
            let right = decode_node(&v["right"], registry, &format!("{path}/union_right"))?;
            Ok(PlanNode::Union {
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        "Join" => {
            let condition = decode_expr(&v["condition"], path)?;
            let left = decode_node(&v["left"], registry, &format!("{path}/join_left"))?;
            let right = decode_node(&v["right"], registry, &format!("{path}/join_right"))?;
            Ok(PlanNode::Join {
                left: Box::new(left),
                right: Box::new(right),
                condition,
            })
        }
        other => Err(CatalogError::Codec(format!(
            "{path}: unknown node type '{other}'"
        ))),
    }
}

fn decode_expr(v: &Value, path: &str) -> Result<Expr, CatalogError> {
    let ty = v["type"]
        .as_str()
        .ok_or_else(|| CatalogError::Codec(format!("{path}: expr missing 'type'")))?;

    match ty {
        "Column" => {
            let idx = v["index"]
                .as_u64()
                .ok_or_else(|| CatalogError::Codec(format!("{path}: Column missing 'index'")))?
                as usize;
            Ok(Expr::Column(idx))
        }
        "Literal" => {
            let bytes = v["bytes"]
                .as_array()
                .ok_or_else(|| CatalogError::Codec(format!("{path}: Literal missing 'bytes'")))?
                .iter()
                .map(|b| {
                    b.as_u64()
                        .ok_or_else(|| CatalogError::Codec(format!("{path}: Literal byte not u64")))
                        .map(|n| n as u8)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::Literal(bytes))
        }
        "BinaryOp" => {
            let op = decode_binary_op(
                v["op"]
                    .as_str()
                    .ok_or_else(|| CatalogError::Codec(format!("{path}: BinaryOp missing 'op'")))?,
                path,
            )?;
            let left = decode_expr(&v["left"], path)?;
            let right = decode_expr(&v["right"], path)?;
            Ok(Expr::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        other => Err(CatalogError::Codec(format!(
            "{path}: unknown expr type '{other}'"
        ))),
    }
}

fn decode_binary_op(s: &str, path: &str) -> Result<BinaryOp, CatalogError> {
    match s {
        "Eq" => Ok(BinaryOp::Eq),
        "Ne" => Ok(BinaryOp::Ne),
        "Lt" => Ok(BinaryOp::Lt),
        "Le" => Ok(BinaryOp::Le),
        "Gt" => Ok(BinaryOp::Gt),
        "Ge" => Ok(BinaryOp::Ge),
        "Add" => Ok(BinaryOp::Add),
        "Sub" => Ok(BinaryOp::Sub),
        "Mul" => Ok(BinaryOp::Mul),
        "Div" => Ok(BinaryOp::Div),
        "And" => Ok(BinaryOp::And),
        "Or" => Ok(BinaryOp::Or),
        other => Err(CatalogError::Codec(format!(
            "{path}: unknown binary op '{other}'"
        ))),
    }
}

fn decode_agg_expr(v: &Value, path: &str) -> Result<AggregateExpr, CatalogError> {
    let func_str = v["func"]
        .as_str()
        .ok_or_else(|| CatalogError::Codec(format!("{path}: agg expr missing 'func'")))?;
    let func = match func_str {
        "Count" => AggregateFunc::Count,
        "Sum" => AggregateFunc::Sum,
        "Avg" => AggregateFunc::Avg,
        "Min" => AggregateFunc::Min,
        "Max" => AggregateFunc::Max,
        other => {
            return Err(CatalogError::Codec(format!(
                "{path}: unknown aggregate function '{other}'"
            )))
        }
    };
    let input = decode_expr(&v["input"], path)?;
    let distinct = v["distinct"].as_bool().unwrap_or(false);
    Ok(AggregateExpr {
        func,
        input,
        distinct,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
    use rockstream_types::laws::registry::LawRegistry;
    use rockstream_types::laws::weight_add::{WEIGHT_ADD_ID, WEIGHT_ADD_VERSION};
    use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

    fn no_law(_: &PlanNode) -> Option<(MergeLawId, MergeLawVersion)> {
        None
    }

    fn weight_add_law(plan: &PlanNode) -> Option<(MergeLawId, MergeLawVersion)> {
        match plan {
            PlanNode::Aggregate { .. } => Some((WEIGHT_ADD_ID, WEIGHT_ADD_VERSION)),
            _ => None,
        }
    }

    #[test]
    fn source_round_trips() {
        let plan = PlanNode::Source {
            name: "orders".into(),
        };
        let registry = LawRegistry::with_builtins();
        let bytes = encode(&plan, &no_law).unwrap();
        let decoded = decode(&bytes, &registry).unwrap();
        assert_eq!(plan, decoded);
    }

    #[test]
    fn filter_round_trips() {
        let plan = PlanNode::Filter {
            input: Box::new(PlanNode::Source {
                name: "events".into(),
            }),
            predicate: Expr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column(0)),
                right: Box::new(Expr::Literal(vec![0, 0, 0, 10])),
            },
        };
        let registry = LawRegistry::with_builtins();
        let bytes = encode(&plan, &no_law).unwrap();
        let decoded = decode(&bytes, &registry).unwrap();
        assert_eq!(plan, decoded);
    }

    #[test]
    fn project_round_trips() {
        let plan = PlanNode::Project {
            input: Box::new(PlanNode::Source { name: "s".into() }),
            columns: vec![Expr::Column(0), Expr::Column(2)],
        };
        let registry = LawRegistry::with_builtins();
        let bytes = encode(&plan, &no_law).unwrap();
        let decoded = decode(&bytes, &registry).unwrap();
        assert_eq!(plan, decoded);
    }

    #[test]
    fn aggregate_with_law_round_trips() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        let registry = LawRegistry::with_builtins();
        let bytes = encode(&plan, &weight_add_law).unwrap();
        // law_id and law_version must be in the bytes
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["plan"]["law_id"], WEIGHT_ADD_ID.0);
        assert_eq!(json["plan"]["law_version"], WEIGHT_ADD_VERSION.0);
        let decoded = decode(&bytes, &registry).unwrap();
        assert_eq!(plan, decoded);
    }

    #[test]
    fn unknown_law_returns_rs_5002() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source { name: "t".into() }),
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                input: Expr::Column(0),
                distinct: false,
            }],
        };
        // Encode with an unknown law_id (0xFFFF is not registered).
        let unknown_law = |_: &PlanNode| -> Option<(MergeLawId, MergeLawVersion)> {
            Some((MergeLawId(0xFFFF), MergeLawVersion(1)))
        };
        let bytes = encode(&plan, &unknown_law).unwrap();

        // Decode with a fresh registry that has NO builtins.
        let empty_registry = LawRegistry::new();
        let err = decode(&bytes, &empty_registry).unwrap_err();
        assert!(
            matches!(err, CatalogError::UnknownMergeLaw { law_id: 0xFFFF, .. }),
            "expected RS-5002 for unknown law, got: {err}"
        );
        assert_eq!(err.error_code(), rockstream_types::error_code::RS_5002);
    }

    #[test]
    fn union_with_law_round_trips() {
        let union_law = |plan: &PlanNode| -> Option<(MergeLawId, MergeLawVersion)> {
            match plan {
                PlanNode::Union { .. } => Some((WEIGHT_ADD_ID, WEIGHT_ADD_VERSION)),
                _ => None,
            }
        };
        let plan = PlanNode::Union {
            left: Box::new(PlanNode::Source { name: "a".into() }),
            right: Box::new(PlanNode::Source { name: "b".into() }),
        };
        let registry = LawRegistry::with_builtins();
        let bytes = encode(&plan, &union_law).unwrap();
        let decoded = decode(&bytes, &registry).unwrap();
        assert_eq!(plan, decoded);
    }

    #[test]
    fn plan_bytes_contain_format_tag() {
        let plan = PlanNode::Source { name: "x".into() };
        let bytes = encode(&plan, &no_law).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["format"], "substrait-ext/rockstream/v1");
        assert_eq!(json["schema_version"], 1);
    }

    #[test]
    fn wrong_format_tag_returns_codec_error() {
        let plan = PlanNode::Source { name: "x".into() };
        let bytes = encode(&plan, &no_law).unwrap();
        // Tamper with the format tag.
        let json_str = std::str::from_utf8(&bytes).unwrap();
        let tampered = json_str.replace(
            "substrait-ext/rockstream/v1",
            "substrait-ext/rockstream/v99",
        );
        let registry = LawRegistry::with_builtins();
        let err = decode(tampered.as_bytes(), &registry).unwrap_err();
        assert!(matches!(err, CatalogError::Codec(_)));
    }
}
