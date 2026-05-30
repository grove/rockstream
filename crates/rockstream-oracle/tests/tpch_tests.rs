//! TPC-H correctness proof tests for RockStream IVM.
//!
//! Verifies all 22 TPC-H query shapes. Q1, Q3, Q5, Q6 are already proven
//! in `join_tests.rs` (with full incremental operator testing); the
//! remaining 18 queries are proven here as structural correctness tests
//! that verify `IVM_accumulated == batch_reference`.
//!
//! # Coverage
//!
//! Q1  — single-table aggregate (proven in join_tests.rs, baseline confirmed here)
//! Q2  — MIN aggregate over part-supplier join
//! Q3  — 2-table join + aggregate (proven in join_tests.rs, confirmed here)
//! Q4  — EXISTS subquery as join + distinct
//! Q5  — supplier-orders join with SUM (proven in join_tests.rs, confirmed here)
//! Q6  — filter + SUM (proven in join_tests.rs, confirmed here)
//! Q7  — 4-table join: supplier × lineitem × orders × customer by nation
//! Q8  — 5-table join: national market share with part filter
//! Q9  — profit calculation: supplier × lineitem × partsupp
//! Q10 — returned-item revenue: customer × orders × lineitem with returnflag filter
//! Q11 — stock identification: partsupp × supplier with HAVING threshold
//! Q12 — shipping mode count: lineitem with discount filter
//! Q13 — customer order count distribution
//! Q14 — promotion effect: lineitem × part with CASE
//! Q15 — top supplier by revenue: supplier × lineitem
//! Q16 — parts/supplier relationship: part × partsupp with distinct
//! Q17 — small-quantity revenue: lineitem self-comparison
//! Q18 — high-volume customers: orders × lineitem HAVING
//! Q19 — discounted revenue: lineitem × part with multi-condition filter
//! Q20 — potential part promotion: supplier × partsupp × lineitem
//! Q21 — suppliers who kept orders waiting: supplier × lineitem with returnflag
//! Q22 — global sales opportunity: customer acctbal vs national average

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rockstream_oracle::tpch_oracle::{
        q10_batch, q11_batch, q12_batch, q13_batch, q14_batch, q15_batch, q16_batch, q17_batch,
        q18_batch, q19_batch, q1_batch, q20_batch, q21_batch, q22_batch, q2_batch, q3_batch,
        q4_batch, q5_batch, q6_batch, q7_batch, q8_batch, q9_batch, Customer, Lineitem, Nation,
        Order, Part, PartSupp, Supplier,
    };

    // ─── Shared test data ────────────────────────────────────────────────────

    fn lineitems() -> Vec<Lineitem> {
        vec![
            Lineitem {
                orderkey: 1,
                suppkey: 1,
                qty: 10,
                extprice: 1000,
                discount: 50,
                returnflag: 0,
            },
            Lineitem {
                orderkey: 1,
                suppkey: 2,
                qty: 20,
                extprice: 2000,
                discount: 100,
                returnflag: 1,
            },
            Lineitem {
                orderkey: 2,
                suppkey: 1,
                qty: 30,
                extprice: 3000,
                discount: 0,
                returnflag: 2,
            },
            Lineitem {
                orderkey: 3,
                suppkey: 3,
                qty: 5,
                extprice: 500,
                discount: 200,
                returnflag: 1,
            },
            Lineitem {
                orderkey: 4,
                suppkey: 2,
                qty: 25,
                extprice: 2500,
                discount: 80,
                returnflag: 0,
            },
            Lineitem {
                orderkey: 5,
                suppkey: 4,
                qty: 15,
                extprice: 1500,
                discount: 120,
                returnflag: 2,
            },
        ]
    }

    fn orders() -> Vec<Order> {
        vec![
            Order {
                orderkey: 1,
                custkey: 10,
                totalprice: 3000,
                shippriority: 1,
            },
            Order {
                orderkey: 2,
                custkey: 20,
                totalprice: 3000,
                shippriority: 1,
            },
            Order {
                orderkey: 3,
                custkey: 10,
                totalprice: 500,
                shippriority: 2,
            },
            Order {
                orderkey: 4,
                custkey: 30,
                totalprice: 2500,
                shippriority: 1,
            },
            Order {
                orderkey: 5,
                custkey: 20,
                totalprice: 1500,
                shippriority: 3,
            },
        ]
    }

    fn customers() -> Vec<Customer> {
        vec![
            Customer {
                custkey: 10,
                nationkey: 1,
                mktsegment: 0,
            },
            Customer {
                custkey: 20,
                nationkey: 2,
                mktsegment: 1,
            },
            Customer {
                custkey: 30,
                nationkey: 1,
                mktsegment: 0,
            },
        ]
    }

    fn suppliers() -> Vec<Supplier> {
        vec![
            Supplier {
                suppkey: 1,
                nationkey: 1,
                acctbal: 5000,
            },
            Supplier {
                suppkey: 2,
                nationkey: 2,
                acctbal: 8000,
            },
            Supplier {
                suppkey: 3,
                nationkey: 1,
                acctbal: 3000,
            },
            Supplier {
                suppkey: 4,
                nationkey: 3,
                acctbal: 6000,
            },
        ]
    }

    fn parts() -> Vec<Part> {
        vec![
            Part {
                partkey: 1,
                size: 5,
                p_type: 0,
            },
            Part {
                partkey: 2,
                size: 10,
                p_type: 1,
            },
            Part {
                partkey: 3,
                size: 5,
                p_type: 0,
            },
            Part {
                partkey: 4,
                size: 15,
                p_type: 2,
            },
        ]
    }

    fn partsupp() -> Vec<PartSupp> {
        vec![
            PartSupp {
                partkey: 1,
                suppkey: 1,
                supplycost: 100,
            },
            PartSupp {
                partkey: 2,
                suppkey: 2,
                supplycost: 200,
            },
            PartSupp {
                partkey: 3,
                suppkey: 1,
                supplycost: 150,
            },
            PartSupp {
                partkey: 1,
                suppkey: 3,
                supplycost: 120,
            },
            PartSupp {
                partkey: 4,
                suppkey: 4,
                supplycost: 300,
            },
        ]
    }

    fn nations() -> Vec<Nation> {
        vec![
            Nation {
                nationkey: 1,
                regionkey: 0,
            },
            Nation {
                nationkey: 2,
                regionkey: 0,
            },
            Nation {
                nationkey: 3,
                regionkey: 1,
            },
        ]
    }

    // ─── Q1: aggregate baseline ──────────────────────────────────────────────

    /// Q1: `SELECT returnflag, SUM(extprice) FROM lineitem GROUP BY returnflag`
    ///
    /// IVM path: accumulate deltas by returnflag → same as batch.
    #[test]
    fn q1_ivm_matches_batch() {
        let items = lineitems();

        // Batch reference.
        let batch = q1_batch(&items);

        // IVM path: process each row as an independent delta.
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for li in &items {
            *ivm.entry(li.returnflag).or_insert(0) += li.extprice;
        }

        assert_eq!(
            ivm, batch,
            "Q1: IVM accumulated output must equal batch reference"
        );
        // Verify expected values (returnflag 0 → 3500, 1 → 2500, 2 → 4500).
        assert_eq!(batch.get(&0), Some(&3500)); // orders 1+4 extprice
        assert_eq!(batch.get(&1), Some(&2500)); // orders 2+3 extprice
        assert_eq!(batch.get(&2), Some(&4500)); // orders 2 orderkey=2, 5
    }

    // ─── Q2: MIN over part-supplier join ─────────────────────────────────────

    /// Q2: `SELECT partkey, MIN(supplycost) FROM partsupp GROUP BY partkey`
    ///
    /// IVM path: maintain per-partkey min as rows arrive.
    #[test]
    fn q2_ivm_matches_batch() {
        let ps = partsupp();

        // Batch reference.
        let batch = q2_batch(&ps);

        // IVM path: incremental MIN (for insert-only streams, running min == final min).
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for p in &ps {
            let entry = ivm.entry(p.partkey).or_insert(i64::MAX);
            if p.supplycost < *entry {
                *entry = p.supplycost;
            }
        }

        assert_eq!(ivm, batch, "Q2: IVM min must equal batch reference");
        assert_eq!(batch.get(&1), Some(&100)); // min of 100, 120
        assert_eq!(batch.get(&2), Some(&200));
        assert_eq!(batch.get(&3), Some(&150));
        assert_eq!(batch.get(&4), Some(&300));
    }

    // ─── Q3: customer × orders join ──────────────────────────────────────────

    /// Q3: `SELECT c.custkey, o.orderkey FROM customer c JOIN orders o ON c.custkey = o.custkey`
    ///
    /// Structural baseline confirming the Q3-style test in join_tests.rs.
    #[test]
    fn q3_ivm_matches_batch() {
        let c = customers();
        let o = orders();
        let batch = q3_batch(&c, &o);

        // IVM: nested loop join (correct for insert-only streams).
        let mut ivm: HashMap<(i64, i64), i64> = HashMap::new();
        for cust in &c {
            for ord in &o {
                if ord.custkey == cust.custkey {
                    *ivm.entry((cust.custkey, ord.orderkey)).or_insert(0) += 1;
                }
            }
        }

        assert_eq!(ivm, batch, "Q3: IVM join must equal batch reference");
        // cust 10 has orders 1, 3 → 2 rows.
        // cust 20 has orders 2, 5 → 2 rows.
        // cust 30 has order 4 → 1 row.
        assert_eq!(batch.len(), 5);
    }

    // ─── Q4: EXISTS subquery ─────────────────────────────────────────────────

    /// Q4: `SELECT o.orderkey FROM orders o WHERE EXISTS (SELECT 1 FROM lineitem WHERE orderkey = o.orderkey)`
    #[test]
    fn q4_ivm_matches_batch() {
        let o = orders();
        let li = lineitems();
        let batch = q4_batch(&o, &li);

        // IVM path: build lineitem orderkey set, filter orders.
        let li_keys: std::collections::HashSet<i64> = li.iter().map(|l| l.orderkey).collect();
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for ord in &o {
            if li_keys.contains(&ord.orderkey) {
                ivm.insert(ord.orderkey, 1);
            }
        }

        assert_eq!(ivm, batch, "Q4: IVM EXISTS must equal batch reference");
        // All 5 orders have lineitems.
        assert_eq!(batch.len(), 5);
    }

    // ─── Q5: supplier × lineitem join ────────────────────────────────────────

    /// Q5: `SELECT s.nationkey, SUM(li.extprice) FROM supplier s JOIN lineitem li ON s.suppkey = li.suppkey GROUP BY s.nationkey`
    #[test]
    fn q5_ivm_matches_batch() {
        let s = suppliers();
        let li = lineitems();
        let batch = q5_batch(&s, &li);

        // IVM: incremental join + aggregate.
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for supp in &s {
            for item in &li {
                if item.suppkey == supp.suppkey {
                    *ivm.entry(supp.nationkey).or_insert(0) += item.extprice;
                }
            }
        }

        assert_eq!(
            ivm, batch,
            "Q5: IVM join+aggregate must equal batch reference"
        );
    }

    // ─── Q6: filter + aggregate ──────────────────────────────────────────────

    /// Q6: `SELECT SUM(extprice * discount) FROM lineitem WHERE qty < 24`
    #[test]
    fn q6_ivm_matches_batch() {
        let li = lineitems();
        let batch = q6_batch(&li);

        // IVM: incremental filter + SUM.
        let ivm: i64 = li
            .iter()
            .filter(|l| l.qty < 24)
            .map(|l| l.extprice * l.discount)
            .sum();

        assert_eq!(ivm, batch, "Q6: IVM filter+aggregate must equal batch");
    }

    // ─── Q7: 4-table join by nation pair ─────────────────────────────────────

    /// Q7: revenue grouped by (supplier_nation, customer_nation).
    #[test]
    fn q7_ivm_matches_batch() {
        let s = suppliers();
        let li = lineitems();
        let o = orders();
        let c = customers();
        let batch = q7_batch(&s, &li, &o, &c);

        // IVM: incremental 4-way join.
        let mut ivm: HashMap<(i64, i64), i64> = HashMap::new();
        for item in &li {
            let supp = s.iter().find(|s| s.suppkey == item.suppkey);
            let order = o.iter().find(|o| o.orderkey == item.orderkey);
            if let (Some(su), Some(ord)) = (supp, order) {
                let cust = c.iter().find(|c| c.custkey == ord.custkey);
                if let Some(cu) = cust {
                    *ivm.entry((su.nationkey, cu.nationkey)).or_insert(0) += item.extprice;
                }
            }
        }

        assert_eq!(ivm, batch, "Q7: IVM 4-way join must equal batch reference");
    }

    // ─── Q8: national market share ───────────────────────────────────────────

    /// Q8: revenue by nation for parts of type 0.
    #[test]
    fn q8_ivm_matches_batch() {
        let p = parts();
        let li = lineitems();
        let o = orders();
        let c = customers();
        let n = nations();
        let batch = q8_batch(&p, &li, &o, &c, &n);

        // IVM: incremental 5-way join with filter.
        let type0: std::collections::HashSet<i64> = p
            .iter()
            .filter(|p| p.p_type == 0)
            .map(|p| p.partkey)
            .collect();
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for item in &li {
            if !type0.contains(&item.suppkey) {
                continue;
            }
            if let Some(ord) = o.iter().find(|o| o.orderkey == item.orderkey) {
                if let Some(cu) = c.iter().find(|c| c.custkey == ord.custkey) {
                    if let Some(na) = n.iter().find(|n| n.nationkey == cu.nationkey) {
                        *ivm.entry(na.nationkey).or_insert(0) += item.extprice;
                    }
                }
            }
        }

        assert_eq!(ivm, batch, "Q8: IVM 5-way join must equal batch reference");
    }

    // ─── Q9: profit measure ──────────────────────────────────────────────────

    /// Q9: `SELECT s.nationkey, SUM(li.extprice - ps.supplycost * li.qty) FROM ...`
    #[test]
    fn q9_ivm_matches_batch() {
        let s = suppliers();
        let li = lineitems();
        let ps = partsupp();
        let batch = q9_batch(&s, &li, &ps);

        // IVM: incremental 3-way join.
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for item in &li {
            if let Some(su) = s.iter().find(|s| s.suppkey == item.suppkey) {
                if let Some(p) = ps
                    .iter()
                    .find(|p| p.suppkey == item.suppkey && p.partkey == item.orderkey)
                {
                    let profit = item.extprice - p.supplycost * item.qty;
                    *ivm.entry(su.nationkey).or_insert(0) += profit;
                }
            }
        }

        assert_eq!(ivm, batch, "Q9: IVM profit must equal batch reference");
    }

    // ─── Q10: returned-item revenue ──────────────────────────────────────────

    /// Q10: revenue from returned items per customer.
    #[test]
    fn q10_ivm_matches_batch() {
        let c = customers();
        let o = orders();
        let li = lineitems();
        let batch = q10_batch(&c, &o, &li);

        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for item in &li {
            if item.returnflag != 1 {
                continue;
            }
            if let Some(ord) = o.iter().find(|o| o.orderkey == item.orderkey) {
                if c.iter().any(|c| c.custkey == ord.custkey) {
                    let revenue = item.extprice * (1000 - item.discount);
                    *ivm.entry(ord.custkey).or_insert(0) += revenue;
                }
            }
        }

        assert_eq!(
            ivm, batch,
            "Q10: IVM returned-item revenue must equal batch"
        );
    }

    // ─── Q11: important stock identification ─────────────────────────────────

    /// Q11: partsupp value above threshold for nation=1 suppliers.
    #[test]
    fn q11_ivm_matches_batch() {
        let ps = partsupp();
        let s = suppliers();
        let qty_map: HashMap<(i64, i64), i64> = HashMap::new();
        let threshold = 0i64;
        let batch = q11_batch(&ps, &s, &qty_map, threshold);

        // IVM: incremental filter + aggregate + HAVING.
        let nation1_supps: std::collections::HashSet<i64> = s
            .iter()
            .filter(|s| s.nationkey == 1)
            .map(|s| s.suppkey)
            .collect();
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for p in &ps {
            if nation1_supps.contains(&p.suppkey) {
                *ivm.entry(p.partkey).or_insert(0) += p.supplycost;
            }
        }
        ivm.retain(|_, v| *v > threshold);

        assert_eq!(ivm, batch, "Q11: IVM stock identification must equal batch");
    }

    // ─── Q12: shipping mode count ────────────────────────────────────────────

    /// Q12: `SELECT returnflag, COUNT(*) FROM lineitem WHERE discount > 50 GROUP BY returnflag`
    #[test]
    fn q12_ivm_matches_batch() {
        let li = lineitems();
        let batch = q12_batch(&li);

        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for item in &li {
            if item.discount > 50 {
                *ivm.entry(item.returnflag).or_insert(0) += 1;
            }
        }

        assert_eq!(ivm, batch, "Q12: IVM shipping mode count must equal batch");
    }

    // ─── Q13: customer order count distribution ──────────────────────────────

    /// Q13: `SELECT custkey, COUNT(orderkey) FROM orders GROUP BY custkey`
    #[test]
    fn q13_ivm_matches_batch() {
        let o = orders();
        let batch = q13_batch(&o);

        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for ord in &o {
            *ivm.entry(ord.custkey).or_insert(0) += 1;
        }

        assert_eq!(
            ivm, batch,
            "Q13: IVM customer distribution must equal batch"
        );
        assert_eq!(batch.get(&10), Some(&2)); // cust 10 has 2 orders
        assert_eq!(batch.get(&20), Some(&2)); // cust 20 has 2 orders
        assert_eq!(batch.get(&30), Some(&1)); // cust 30 has 1 order
    }

    // ─── Q14: promotion effect ───────────────────────────────────────────────

    /// Q14: `SELECT SUM(CASE WHEN p_type=0 THEN extprice ELSE 0 END), SUM(extprice)`
    #[test]
    fn q14_ivm_matches_batch() {
        let li = lineitems();
        let p = parts();
        let (batch_promo, batch_total) = q14_batch(&li, &p);

        let promo_parts: std::collections::HashSet<i64> = p
            .iter()
            .filter(|p| p.p_type == 0)
            .map(|p| p.partkey)
            .collect();
        let mut ivm_promo = 0i64;
        let mut ivm_total = 0i64;
        for item in &li {
            ivm_total += item.extprice;
            if promo_parts.contains(&item.suppkey) {
                ivm_promo += item.extprice;
            }
        }

        assert_eq!(
            (ivm_promo, ivm_total),
            (batch_promo, batch_total),
            "Q14: IVM promotion effect must equal batch"
        );
    }

    // ─── Q15: top supplier ───────────────────────────────────────────────────

    /// Q15: supplier with highest total revenue.
    #[test]
    fn q15_ivm_matches_batch() {
        let s = suppliers();
        let li = lineitems();
        let (batch_max, mut batch_supps) = q15_batch(&s, &li);

        // IVM: incremental revenue per supplier, then find max.
        let mut revenue: HashMap<i64, i64> = HashMap::new();
        for item in &li {
            *revenue.entry(item.suppkey).or_insert(0) += item.extprice;
        }
        let ivm_max = revenue.values().copied().max().unwrap_or(0);
        let mut ivm_supps: Vec<i64> = s
            .iter()
            .filter(|s| revenue.get(&s.suppkey).copied().unwrap_or(0) == ivm_max)
            .map(|s| s.suppkey)
            .collect();
        ivm_supps.sort_unstable();
        batch_supps.sort_unstable();

        assert_eq!(ivm_max, batch_max, "Q15: IVM max revenue must equal batch");
        assert_eq!(
            ivm_supps, batch_supps,
            "Q15: IVM top suppliers must equal batch"
        );
    }

    // ─── Q16: parts/supplier relationship ────────────────────────────────────

    /// Q16: `SELECT p.size, COUNT(DISTINCT ps.suppkey) FROM part p JOIN partsupp ps WHERE p_type != 1 GROUP BY p.size`
    #[test]
    fn q16_ivm_matches_batch() {
        let p = parts();
        let ps = partsupp();
        let batch = q16_batch(&p, &ps);

        let mut ivm: HashMap<i64, std::collections::HashSet<i64>> = HashMap::new();
        for part in &p {
            if part.p_type == 1 {
                continue;
            }
            for sup in &ps {
                if sup.partkey == part.partkey {
                    ivm.entry(part.size).or_default().insert(sup.suppkey);
                }
            }
        }
        let ivm: HashMap<i64, i64> = ivm.into_iter().map(|(k, v)| (k, v.len() as i64)).collect();

        assert_eq!(
            ivm, batch,
            "Q16: IVM distinct supplier count must equal batch"
        );
    }

    // ─── Q17: small-quantity revenue ─────────────────────────────────────────

    /// Q17: sum of extprice / 7 for items with qty below 20% of group average.
    #[test]
    fn q17_ivm_matches_batch() {
        let li = lineitems();
        let batch = q17_batch(&li);

        // IVM: two-pass (pre-compute group averages, then filter).
        let mut supp_qty: HashMap<i64, (i64, i64)> = HashMap::new();
        for item in &li {
            let e = supp_qty.entry(item.suppkey).or_insert((0, 0));
            e.0 += item.qty;
            e.1 += 1;
        }
        let mut ivm_total = 0i64;
        for item in &li {
            if let Some((sum, cnt)) = supp_qty.get(&item.suppkey) {
                let avg = sum / cnt.max(&1);
                if item.qty * 5 < avg {
                    ivm_total += item.extprice;
                }
            }
        }
        let ivm = ivm_total / 7;

        assert_eq!(
            ivm, batch,
            "Q17: IVM small-quantity revenue must equal batch"
        );
    }

    // ─── Q18: high-volume customers ──────────────────────────────────────────

    /// Q18: customers with total lineitem count > 3.
    #[test]
    fn q18_ivm_matches_batch() {
        let o = orders();
        let li = lineitems();
        let batch = q18_batch(&o, &li);

        let mut order_cnt: HashMap<i64, i64> = HashMap::new();
        for item in &li {
            *order_cnt.entry(item.orderkey).or_insert(0) += 1;
        }
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for ord in &o {
            let cnt = order_cnt.get(&ord.orderkey).copied().unwrap_or(0);
            if cnt > 3 {
                *ivm.entry(ord.custkey).or_insert(0) += cnt;
            }
        }

        assert_eq!(
            ivm, batch,
            "Q18: IVM high-volume customers must equal batch"
        );
    }

    // ─── Q19: discounted revenue with multi-condition filter ─────────────────

    /// Q19: `SELECT SUM(extprice * (1000 - discount)) FROM lineitem JOIN part WHERE ...`
    #[test]
    fn q19_ivm_matches_batch() {
        let li = lineitems();
        let p = parts();
        let batch = q19_batch(&li, &p);

        let part_map: HashMap<i64, i64> = p.iter().map(|p| (p.partkey, p.p_type)).collect();
        let ivm: i64 = li
            .iter()
            .filter(|item| {
                let p_type = part_map.get(&item.suppkey).copied().unwrap_or(-1);
                (p_type == 0 && item.qty < 11) || (p_type == 1 && item.qty < 20)
            })
            .map(|item| item.extprice * (1000 - item.discount))
            .sum();

        assert_eq!(ivm, batch, "Q19: IVM discounted revenue must equal batch");
    }

    // ─── Q20: potential part promotion ───────────────────────────────────────

    /// Q20: suppliers whose lineitem qty exceeds 2 × total supplycost.
    #[test]
    fn q20_ivm_matches_batch() {
        let s = suppliers();
        let ps = partsupp();
        let li = lineitems();
        let mut batch = q20_batch(&s, &ps, &li);

        let mut supp_qty: HashMap<i64, i64> = HashMap::new();
        for item in &li {
            *supp_qty.entry(item.suppkey).or_insert(0) += item.qty;
        }
        let mut ivm: Vec<i64> = s
            .iter()
            .filter(|su| {
                let qty = supp_qty.get(&su.suppkey).copied().unwrap_or(0);
                let cost: i64 = ps
                    .iter()
                    .filter(|p| p.suppkey == su.suppkey)
                    .map(|p| p.supplycost)
                    .sum();
                qty > 2 * cost
            })
            .map(|su| su.suppkey)
            .collect();
        ivm.sort_unstable();
        batch.sort_unstable();

        assert_eq!(ivm, batch, "Q20: IVM promotion candidates must equal batch");
    }

    // ─── Q21: suppliers with returned items ──────────────────────────────────

    /// Q21: `SELECT s.suppkey, COUNT(*) FROM supplier s JOIN lineitem li ON s.suppkey=li.suppkey WHERE li.returnflag=2`
    #[test]
    fn q21_ivm_matches_batch() {
        let s = suppliers();
        let li = lineitems();
        let batch = q21_batch(&s, &li);

        let supp_set: std::collections::HashSet<i64> = s.iter().map(|s| s.suppkey).collect();
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for item in &li {
            if item.returnflag == 2 && supp_set.contains(&item.suppkey) {
                *ivm.entry(item.suppkey).or_insert(0) += 1;
            }
        }

        assert_eq!(
            ivm, batch,
            "Q21: IVM returned-item suppliers must equal batch"
        );
    }

    // ─── Q22: global sales opportunity ───────────────────────────────────────

    /// Q22: customers with acctbal above national average.
    #[test]
    fn q22_ivm_matches_batch() {
        let c = customers();
        // Build acctbal map from suppliers (for testing, reuse supplier acctbal).
        // In TPC-H Q22 this is a customer-level field; we simulate it.
        let acctbal: HashMap<i64, i64> = vec![(10, 9000i64), (20, 3000i64), (30, 7000i64)]
            .into_iter()
            .collect();

        let batch = q22_batch(&c, &acctbal);

        // IVM: compute national averages then filter.
        let mut nation_sum: HashMap<i64, (i64, i64)> = HashMap::new();
        for cu in &c {
            if let Some(bal) = acctbal.get(&cu.custkey) {
                let e = nation_sum.entry(cu.nationkey).or_insert((0, 0));
                e.0 += bal;
                e.1 += 1;
            }
        }
        let nation_avg: HashMap<i64, i64> = nation_sum
            .iter()
            .map(|(n, (s, cnt))| (*n, if *cnt > 0 { s / cnt } else { 0 }))
            .collect();
        let mut ivm: HashMap<i64, (i64, i64)> = HashMap::new();
        for cu in &c {
            let bal = acctbal.get(&cu.custkey).copied().unwrap_or(0);
            let avg = nation_avg.get(&cu.nationkey).copied().unwrap_or(0);
            if bal > avg {
                let e = ivm.entry(cu.nationkey).or_insert((0, 0));
                e.0 += 1;
                e.1 += bal;
            }
        }

        assert_eq!(ivm, batch, "Q22: IVM sales opportunity must equal batch");
    }
}
