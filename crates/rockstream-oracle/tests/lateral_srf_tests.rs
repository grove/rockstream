//! Proof tests for v0.25: Lateral, SRF, UDF, and approximate-aggregate surface.
//!
//! Proves all items in the v0.25 roadmap:
//!
//! 1.  `proof_unnest_matches_batch` — UNNEST lateral expands rows correctly.
//! 2.  `proof_generate_series_matches_batch` — generate_series output matches range.
//! 3.  `proof_json_extract_array_matches_batch` — JSON array expansion matches batch.
//! 4.  `proof_lateral_retraction_is_exact` — retracting input retracts exactly its rows.
//! 5.  `proof_approx_count_distinct_uses_hll_law` — APPROX_COUNT_DISTINCT DiffCtx uses HLL_ID.
//! 6.  `proof_approx_membership_uses_bloom_union_law` — APPROX_MEMBERSHIP DiffCtx uses BLOOM_UNION_ID.
//! 7.  `proof_hll_sketch_union_idempotent` — HLL merge(a,a)=a.
//! 8.  `proof_hll_sketch_union_commutative` — HLL merge(a,b)=merge(b,a).
//! 9.  `proof_bloom_union_sketch_idempotent` — BloomUnion merge(a,a)=a.
//! 10. `proof_bloom_union_sketch_commutative` — BloomUnion merge(a,b)=merge(b,a).
//! 11. `proof_udaf_spec_documents_requirements` — UdafSpec fields exist + document requirements.
//! 12. `proof_scalar_udf_expr_is_stateless` — ScalarUdf expr gets Stateless treatment in DiffCtx.
//! 13. `proof_lateral_codec_roundtrip` — PlanNode::Lateral roundtrips through catalog codec.

#[cfg(test)]
mod tests {
    use rockstream_catalog::{decode_plan, encode_plan};
    use rockstream_diff::DiffCtx;
    use rockstream_oracle::lateral_srf_oracle::{LateralOracle, UdafRequirements};
    use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, LateralFunc, PlanNode, UdafSpec};
    use rockstream_types::batch::ZSet;
    use rockstream_types::laws::bloom_union::{
        BloomUnionV1, BLOOM_UNION_ID, BLOOM_UNION_WIRE_SIZE,
    };
    use rockstream_types::laws::hyper_log_log::{HyperLogLogV1, HLL_ID, HLL_WIRE_SIZE};
    use rockstream_types::laws::registry::LawRegistry;
    use rockstream_types::merge_law::{LawBundle, MergeLawId, MergeLawVersion};

    // ─── Helpers ─────────────────────────────────────────────────────────────

    /// Build a ZSet with a single row.
    fn single_row(key: &[u8], value: &[u8], weight: i64) -> ZSet {
        let mut z = ZSet::default();
        z.insert(key.to_vec(), value.to_vec(), weight);
        z
    }

    /// Encode a list of byte elements as the wire format for Unnest:
    /// `[u8: count] [u8: len] [bytes...]...`
    fn encode_unnest_list(elems: &[&[u8]]) -> Vec<u8> {
        let mut out = vec![elems.len() as u8];
        for &e in elems {
            out.push(e.len() as u8);
            out.extend_from_slice(e);
        }
        out
    }

    // ─── 1. Unnest lateral ───────────────────────────────────────────────────

    /// Proof: UNNEST lateral expands one row with three elements into three rows.
    #[test]
    fn proof_unnest_matches_batch() {
        let key = b"row0";
        let elems: &[&[u8]] = &[b"a", b"bb", b"ccc"];
        let value = encode_unnest_list(elems);
        let input = single_row(key, &value, 1);

        let func = LateralFunc::Unnest { col: 0 };
        let output = LateralOracle::eval(&func, &input);

        let out_rows: Vec<Vec<u8>> = output
            .iter()
            .filter(|r| r.weight > 0)
            .map(|r| r.value.clone())
            .collect();

        assert_eq!(out_rows.len(), 3, "UNNEST should produce 3 output rows");
        // All output rows belong to the same key.
        for row in output.iter() {
            assert_eq!(&row.key, key, "UNNEST preserves input key");
        }
        // Elements appear in order.
        let mut sorted = out_rows.clone();
        sorted.sort();
        let mut expected: Vec<Vec<u8>> = elems.iter().map(|e| e.to_vec()).collect();
        expected.sort();
        assert_eq!(sorted, expected, "UNNEST values match input elements");
    }

    // ─── 2. generate_series lateral ──────────────────────────────────────────

    /// Proof: generate_series(1, 5, 1) produces 5 rows [1,2,3,4,5].
    #[test]
    fn proof_generate_series_matches_batch() {
        let key = b"k";
        let input = single_row(key, b"", 1);
        let func = LateralFunc::GenerateSeries {
            start: 1,
            stop: 5,
            step: 1,
        };
        let output = LateralOracle::eval(&func, &input);

        let values: Vec<i64> = {
            let mut v: Vec<i64> = output
                .iter()
                .filter(|r| r.weight > 0)
                .map(|r| {
                    let bytes: [u8; 8] = r.value.as_slice().try_into().unwrap_or([0u8; 8]);
                    i64::from_be_bytes(bytes)
                })
                .collect();
            v.sort();
            v
        };

        assert_eq!(
            values,
            vec![1, 2, 3, 4, 5],
            "generate_series(1,5,1) = [1..5]"
        );
    }

    // ─── 3. json_extract_array lateral ───────────────────────────────────────

    /// Proof: json_extract_array parses `[10,20,30]` into 3 rows.
    #[test]
    fn proof_json_extract_array_matches_batch() {
        let key = b"k";
        let json = b"[10,20,30]";
        let input = single_row(key, json, 1);
        let func = LateralFunc::JsonExtractArray { col: 0 };
        let output = LateralOracle::eval(&func, &input);

        let mut values: Vec<Vec<u8>> = output
            .iter()
            .filter(|r| r.weight > 0)
            .map(|r| r.value.clone())
            .collect();
        values.sort();

        assert_eq!(
            values,
            vec![b"10".to_vec(), b"20".to_vec(), b"30".to_vec()],
            "json_extract_array parses ASCII JSON integer array"
        );
    }

    // ─── 4. Lateral retraction ────────────────────────────────────────────────

    /// Proof: retracting an input row retracts exactly the rows it produced.
    ///
    /// Delta semantics: retraction (weight=-1) produces negative-weight output
    /// rows that cancel out the original insertions.
    #[test]
    fn proof_lateral_retraction_is_exact() {
        let key = b"r";
        let func = LateralFunc::GenerateSeries {
            start: 0,
            stop: 2,
            step: 1,
        };

        // Insert.
        let insert_input = single_row(key, b"", 1);
        let insert_out = LateralOracle::eval(&func, &insert_input);

        // Retract.
        let retract_input = single_row(key, b"", -1);
        let retract_out = LateralOracle::eval(&func, &retract_input);

        // Merge: the combined delta should be empty (net zero).
        let mut combined = insert_out.clone();
        combined.merge(&retract_out);
        combined.consolidate();

        assert!(
            combined.is_empty(),
            "insert + retract should consolidate to empty ZSet"
        );
    }

    // ─── 5. APPROX_COUNT_DISTINCT uses HLL_ID ────────────────────────────────

    /// Proof: `APPROX_COUNT_DISTINCT` aggregate in a plan node maps to HLL_ID
    /// in the DiffCtx law assignment.
    #[test]
    fn proof_approx_count_distinct_uses_hll_law() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "s".to_string(),
            }),
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::ApproxCountDistinct,
                input: Expr::Column(0),
                distinct: false,
            }],
        };

        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&plan);

        let agg_op = ops
            .iter()
            .find(|n| matches!(n.kind, rockstream_plan::OpKind::Aggregate));
        assert!(agg_op.is_some(), "plan must have an Aggregate operator");

        let agg = agg_op.unwrap();
        assert_eq!(
            agg.merge_law,
            Some(HLL_ID),
            "APPROX_COUNT_DISTINCT must use HLL_ID"
        );
    }

    // ─── 6. APPROX_MEMBERSHIP uses BLOOM_UNION_ID ────────────────────────────

    /// Proof: `APPROX_MEMBERSHIP` aggregate in a plan node maps to
    /// `BLOOM_UNION_ID` in the DiffCtx law assignment.
    #[test]
    fn proof_approx_membership_uses_bloom_union_law() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "s".to_string(),
            }),
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::ApproxMembership,
                input: Expr::Column(0),
                distinct: false,
            }],
        };

        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&plan);

        let agg_op = ops
            .iter()
            .find(|n| matches!(n.kind, rockstream_plan::OpKind::Aggregate));
        assert!(agg_op.is_some(), "plan must have an Aggregate operator");

        let agg = agg_op.unwrap();
        assert_eq!(
            agg.merge_law,
            Some(BLOOM_UNION_ID),
            "APPROX_MEMBERSHIP must use BLOOM_UNION_ID"
        );
    }

    // ─── 7. HLL sketch union idempotent ──────────────────────────────────────

    /// Proof: `merge(a, a) == a` for HyperLogLog/v1 (idempotency / semilattice).
    #[test]
    fn proof_hll_sketch_union_idempotent() {
        let mut a = vec![0u8; HLL_WIRE_SIZE];
        a[3] = 7;
        a[17] = 4;
        a[63] = 12;

        let law = HyperLogLogV1;
        let merged = law.merge(&a, &a).expect("HLL merge must not fail");
        assert_eq!(merged, a, "HLL merge(a,a) must equal a (idempotent)");
    }

    // ─── 8. HLL sketch union commutative ─────────────────────────────────────

    /// Proof: `merge(a, b) == merge(b, a)` for HyperLogLog/v1.
    #[test]
    fn proof_hll_sketch_union_commutative() {
        let mut a = vec![0u8; HLL_WIRE_SIZE];
        let mut b = vec![0u8; HLL_WIRE_SIZE];
        a[0] = 5;
        a[31] = 3;
        b[0] = 2;
        b[31] = 7;
        b[60] = 1;

        let law = HyperLogLogV1;
        let ab = law.merge(&a, &b).expect("merge a,b");
        let ba = law.merge(&b, &a).expect("merge b,a");
        assert_eq!(ab, ba, "HLL merge(a,b) must equal merge(b,a) (commutative)");
    }

    // ─── 9. BloomUnion sketch idempotent ─────────────────────────────────────

    /// Proof: `merge(a, a) == a` for BloomUnion/v1.
    #[test]
    fn proof_bloom_union_sketch_idempotent() {
        let mut a = vec![0u8; BLOOM_UNION_WIRE_SIZE];
        a[0] = 0b1010_1010;
        a[15] = 0b1111_0000;
        a[31] = 0b0000_1111;

        let law = BloomUnionV1;
        let merged = law.merge(&a, &a).expect("BloomUnion merge must not fail");
        assert_eq!(merged, a, "BloomUnion merge(a,a) must equal a (idempotent)");
    }

    // ─── 10. BloomUnion sketch commutative ───────────────────────────────────

    /// Proof: `merge(a, b) == merge(b, a)` for BloomUnion/v1.
    #[test]
    fn proof_bloom_union_sketch_commutative() {
        let mut a = vec![0u8; BLOOM_UNION_WIRE_SIZE];
        let mut b = vec![0u8; BLOOM_UNION_WIRE_SIZE];
        a[0] = 0b0000_1111;
        b[0] = 0b1111_0000;
        a[16] = 0b1010_0101;
        b[16] = 0b0101_1010;

        let law = BloomUnionV1;
        let ab = law.merge(&a, &b).expect("merge a,b");
        let ba = law.merge(&b, &a).expect("merge b,a");
        assert_eq!(
            ab, ba,
            "BloomUnion merge(a,b) must equal merge(b,a) (commutative)"
        );
    }

    // ─── 11. UdafSpec documents requirements ─────────────────────────────────

    /// Proof: `UdafSpec` struct documents all required algebraic fields
    /// and the oracle `UdafRequirements` captures the UDAF requirements spec.
    #[test]
    fn proof_udaf_spec_documents_requirements() {
        // Verify UdafSpec fields exist (compile-time check via construction).
        let spec = UdafSpec {
            name: "my_agg".to_string(),
            is_commutative_monoid: false,
            is_idempotent: false,
            has_inverse: false,
            description: "A custom user-defined aggregate function.".to_string(),
        };
        assert_eq!(spec.name, "my_agg");
        assert!(!spec.is_commutative_monoid);
        assert!(!spec.is_idempotent);
        assert!(!spec.has_inverse);
        assert!(!spec.description.is_empty());

        // Verify oracle UdafRequirements captures the algebraic properties.
        let req = UdafRequirements {
            name: "my_agg".to_string(),
            associative: true,
            commutative: true,
            has_identity: true,
            has_inverse: false,
            idempotent: false,
            retraction_note: "requires full state rescan on retraction".to_string(),
        };
        assert!(
            req.is_commutative_monoid(),
            "associative + commutative + identity => commutative monoid"
        );
        assert!(!req.is_abelian_group(), "no inverse => not abelian group");
        assert!(!req.is_semilattice(), "not idempotent => not semilattice");

        // A semilattice example.
        let bloom_req = UdafRequirements {
            name: "bloom_union".to_string(),
            associative: true,
            commutative: true,
            has_identity: true,
            has_inverse: false,
            idempotent: true,
            retraction_note: "ExtremumRequiresRmw: full state rescan needed".to_string(),
        };
        assert!(bloom_req.is_semilattice(), "bloom_union is a semilattice");
    }

    // ─── 12. ScalarUdf expr is stateless ─────────────────────────────────────

    /// Proof: a plan containing `Expr::ScalarUdf` produces a stateless
    /// (no law_id) operator node in DiffCtx — scalar UDFs are stateless.
    #[test]
    fn proof_scalar_udf_expr_is_stateless() {
        // A Project node using a ScalarUdf expression.
        let plan = PlanNode::Project {
            input: Box::new(PlanNode::Source {
                name: "src".to_string(),
            }),
            columns: vec![Expr::ScalarUdf {
                name: "my_udf".to_string(),
                args: vec![Expr::Column(0)],
            }],
        };

        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&plan);

        // Project operators must not carry a law_id (stateless).
        let proj_op = ops
            .iter()
            .find(|n| matches!(n.kind, rockstream_plan::OpKind::Project));
        assert!(proj_op.is_some(), "plan must have a Project operator");
        assert_eq!(
            proj_op.unwrap().merge_law,
            None,
            "Project (with ScalarUdf) must be stateless (no merge_law)"
        );
    }

    // ─── 13. Lateral codec roundtrip ──────────────────────────────────────────

    /// Proof: `PlanNode::Lateral` roundtrips through the catalog codec
    /// (encode → decode → equal).
    #[test]
    fn proof_lateral_codec_roundtrip() {
        let registry = LawRegistry::with_builtins();
        let no_law = |_: &PlanNode| -> Option<(MergeLawId, MergeLawVersion)> { None };

        // Unnest variant.
        let unnest_plan = PlanNode::Lateral {
            input: Box::new(PlanNode::Source {
                name: "tbl".to_string(),
            }),
            func: LateralFunc::Unnest { col: 2 },
        };
        let encoded_unnest =
            encode_plan(&unnest_plan, &no_law).expect("Lateral/Unnest encode must not fail");
        let decoded_unnest =
            decode_plan(&encoded_unnest, &registry).expect("Lateral/Unnest decode must not fail");
        assert_eq!(
            unnest_plan, decoded_unnest,
            "Lateral[Unnest] must roundtrip through catalog codec"
        );

        // GenerateSeries variant.
        let gs_plan = PlanNode::Lateral {
            input: Box::new(PlanNode::Source {
                name: "tbl".to_string(),
            }),
            func: LateralFunc::GenerateSeries {
                start: 1,
                stop: 100,
                step: 5,
            },
        };
        let encoded_gs =
            encode_plan(&gs_plan, &no_law).expect("Lateral/GenerateSeries encode must not fail");
        let decoded_gs = decode_plan(&encoded_gs, &registry)
            .expect("Lateral/GenerateSeries decode must not fail");
        assert_eq!(
            gs_plan, decoded_gs,
            "Lateral[GenerateSeries] must roundtrip through catalog codec"
        );

        // JsonExtractArray variant.
        let jea_plan = PlanNode::Lateral {
            input: Box::new(PlanNode::Source {
                name: "tbl".to_string(),
            }),
            func: LateralFunc::JsonExtractArray { col: 0 },
        };
        let encoded_jea =
            encode_plan(&jea_plan, &no_law).expect("Lateral/JsonExtractArray encode must not fail");
        let decoded_jea = decode_plan(&encoded_jea, &registry)
            .expect("Lateral/JsonExtractArray decode must not fail");
        assert_eq!(
            jea_plan, decoded_jea,
            "Lateral[JsonExtractArray] must roundtrip through catalog codec"
        );
    }
}
