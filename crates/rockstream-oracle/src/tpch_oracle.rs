//! TPC-H correctness oracle for RockStream IVM.
//!
//! Provides schema helpers and batch reference evaluators for all 22 TPC-H
//! query shapes. Each query is modelled as a composition of Filter + Join +
//! Aggregate operators, with the batch reference computing ground-truth over
//! the accumulated ZSet state.
//!
//! # Scope
//!
//! These are *structural* proof tests — they use simplified versions of the
//! TPC-H schemas and verify IVM correctness (incremental == batch) for each
//! query's computation pattern. The key correctness criterion is:
//!
//! ```text
//! accumulate(IVM_delta_stream) == batch_reference(full_state)
//! ```
//!
//! for each TPC-H query shape Q1–Q22.
//!
//! # Encoding
//!
//! All rows use a compact binary encoding: key = 8-byte big-endian i64
//! primary key; value = N × 8-byte big-endian i64 fields (left to right).

use std::collections::HashMap;

// ─── Encoding helpers ────────────────────────────────────────────────────────

/// Encode an i64 as 8 big-endian bytes.
pub fn enc(v: i64) -> [u8; 8] {
    v.to_be_bytes()
}

/// Decode the first 8 bytes of a slice as a big-endian i64.
pub fn dec(b: &[u8]) -> i64 {
    if b.len() >= 8 {
        i64::from_be_bytes(b[..8].try_into().unwrap_or([0u8; 8]))
    } else {
        0
    }
}

/// Encode a row with a scalar key and up to 4 value fields.
pub fn encode_row(key: i64, fields: &[i64]) -> (Vec<u8>, Vec<u8>) {
    let k = enc(key).to_vec();
    let mut v = Vec::with_capacity(fields.len() * 8);
    for f in fields {
        v.extend_from_slice(&enc(*f));
    }
    (k, v)
}

/// Decode a value into N i64 fields.
pub fn decode_fields(value: &[u8], n: usize) -> Vec<i64> {
    (0..n)
        .map(|i| {
            let start = i * 8;
            if value.len() >= start + 8 {
                dec(&value[start..])
            } else {
                0
            }
        })
        .collect()
}

// ─── TPC-H row types ─────────────────────────────────────────────────────────

/// A lineitem row: (orderkey, suppkey, qty, extprice, discount, returnflag).
#[derive(Clone, Debug)]
pub struct Lineitem {
    pub orderkey: i64,
    pub suppkey: i64,
    pub qty: i64,
    pub extprice: i64,
    pub discount: i64,   // stored as 1000× fraction, e.g. 50 = 5%
    pub returnflag: i64, // 0=N, 1=R, 2=A
}

impl Lineitem {
    /// Encode as `(key=orderkey, value=[suppkey, qty, extprice, discount, returnflag])`.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        encode_row(
            self.orderkey,
            &[
                self.suppkey,
                self.qty,
                self.extprice,
                self.discount,
                self.returnflag,
            ],
        )
    }

    /// Decode from `(key, value)`.
    pub fn decode(key: &[u8], value: &[u8]) -> Self {
        let fields = decode_fields(value, 5);
        Lineitem {
            orderkey: dec(key),
            suppkey: fields[0],
            qty: fields[1],
            extprice: fields[2],
            discount: fields[3],
            returnflag: fields[4],
        }
    }
}

/// An orders row: (orderkey, custkey, totalprice, shippriority).
#[derive(Clone, Debug)]
pub struct Order {
    pub orderkey: i64,
    pub custkey: i64,
    pub totalprice: i64,
    pub shippriority: i64,
}

impl Order {
    /// Encode as `(key=orderkey, value=[custkey, totalprice, shippriority])`.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        encode_row(
            self.orderkey,
            &[self.custkey, self.totalprice, self.shippriority],
        )
    }

    /// Decode from `(key, value)`.
    pub fn decode(key: &[u8], value: &[u8]) -> Self {
        let fields = decode_fields(value, 3);
        Order {
            orderkey: dec(key),
            custkey: fields[0],
            totalprice: fields[1],
            shippriority: fields[2],
        }
    }
}

/// A customer row: (custkey, nationkey, mktsegment).
#[derive(Clone, Debug)]
pub struct Customer {
    pub custkey: i64,
    pub nationkey: i64,
    pub mktsegment: i64, // encoded as i64 (0=BUILDING, 1=AUTOMOBILE, …)
}

impl Customer {
    /// Encode as `(key=custkey, value=[nationkey, mktsegment])`.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        encode_row(self.custkey, &[self.nationkey, self.mktsegment])
    }
}

/// A supplier row: (suppkey, nationkey, acctbal).
#[derive(Clone, Debug)]
pub struct Supplier {
    pub suppkey: i64,
    pub nationkey: i64,
    pub acctbal: i64,
}

impl Supplier {
    /// Encode as `(key=suppkey, value=[nationkey, acctbal])`.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        encode_row(self.suppkey, &[self.nationkey, self.acctbal])
    }
}

/// A part row: (partkey, size, p_type).
#[derive(Clone, Debug)]
pub struct Part {
    pub partkey: i64,
    pub size: i64,
    pub p_type: i64, // encoded as i64 (0=BRASS, 1=COPPER, …)
}

impl Part {
    /// Encode as `(key=partkey, value=[size, p_type])`.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        encode_row(self.partkey, &[self.size, self.p_type])
    }
}

/// A partsupp row: (partkey, suppkey, supplycost).
#[derive(Clone, Debug)]
pub struct PartSupp {
    pub partkey: i64,
    pub suppkey: i64,
    pub supplycost: i64,
}

impl PartSupp {
    /// Encode as `(key=partkey, value=[suppkey, supplycost])`.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        encode_row(self.partkey, &[self.suppkey, self.supplycost])
    }
}

/// A nation row: (nationkey, regionkey).
#[derive(Clone, Debug)]
pub struct Nation {
    pub nationkey: i64,
    pub regionkey: i64,
}

impl Nation {
    /// Encode as `(key=nationkey, value=[regionkey])`.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        encode_row(self.nationkey, &[self.regionkey])
    }
}

// ─── TPC-H batch reference evaluators ────────────────────────────────────────

/// Evaluate Q1-style: `SELECT returnflag, SUM(extprice) FROM lineitem GROUP BY returnflag`.
///
/// Returns `HashMap<returnflag, sum_extprice>`.
pub fn q1_batch(items: &[Lineitem]) -> HashMap<i64, i64> {
    let mut result: HashMap<i64, i64> = HashMap::new();
    for li in items {
        *result.entry(li.returnflag).or_insert(0) += li.extprice;
    }
    result
}

/// Evaluate Q2-style: `SELECT partkey, MIN(supplycost) FROM partsupp GROUP BY partkey`.
///
/// Returns `HashMap<partkey, min_supplycost>`.
pub fn q2_batch(partsupp: &[PartSupp]) -> HashMap<i64, i64> {
    let mut result: HashMap<i64, i64> = HashMap::new();
    for ps in partsupp {
        let entry = result.entry(ps.partkey).or_insert(i64::MAX);
        if ps.supplycost < *entry {
            *entry = ps.supplycost;
        }
    }
    result
}

/// Evaluate Q3-style: `SELECT c.custkey, o.orderkey, COUNT(*) FROM customer c
/// JOIN orders o ON c.custkey = o.custkey GROUP BY c.custkey, o.orderkey`.
///
/// Returns `HashMap<(custkey, orderkey), count>`.
pub fn q3_batch(customers: &[Customer], orders: &[Order]) -> HashMap<(i64, i64), i64> {
    let mut result: HashMap<(i64, i64), i64> = HashMap::new();
    for c in customers {
        for o in orders {
            if o.custkey == c.custkey {
                *result.entry((c.custkey, o.orderkey)).or_insert(0) += 1;
            }
        }
    }
    result
}

/// Evaluate Q4-style: `SELECT o.orderkey, COUNT(*) FROM orders o
/// WHERE EXISTS (SELECT 1 FROM lineitem li WHERE li.orderkey = o.orderkey)
/// GROUP BY o.orderkey`.
///
/// Returns `HashMap<orderkey, 1>` for all orders that have at least one lineitem.
pub fn q4_batch(orders: &[Order], items: &[Lineitem]) -> HashMap<i64, i64> {
    let item_orderkeys: std::collections::HashSet<i64> =
        items.iter().map(|li| li.orderkey).collect();
    let mut result: HashMap<i64, i64> = HashMap::new();
    for o in orders {
        if item_orderkeys.contains(&o.orderkey) {
            result.insert(o.orderkey, 1);
        }
    }
    result
}

/// Evaluate Q5-style: `SELECT s.nationkey, SUM(li.extprice) FROM supplier s
/// JOIN lineitem li ON s.suppkey = li.suppkey GROUP BY s.nationkey`.
///
/// Returns `HashMap<nationkey, sum_extprice>`.
pub fn q5_batch(suppliers: &[Supplier], items: &[Lineitem]) -> HashMap<i64, i64> {
    let mut result: HashMap<i64, i64> = HashMap::new();
    for s in suppliers {
        for li in items {
            if li.suppkey == s.suppkey {
                *result.entry(s.nationkey).or_insert(0) += li.extprice;
            }
        }
    }
    result
}

/// Evaluate Q6-style: `SELECT SUM(extprice * discount) FROM lineitem WHERE qty < 24`.
///
/// Returns the single aggregate value.
pub fn q6_batch(items: &[Lineitem]) -> i64 {
    items
        .iter()
        .filter(|li| li.qty < 24)
        .map(|li| li.extprice * li.discount)
        .sum()
}

/// Evaluate Q7-style: `SELECT n1.nationkey, n2.nationkey, SUM(li.extprice)
/// FROM supplier s JOIN lineitem li ON s.suppkey = li.suppkey
/// JOIN nation n1 ON s.nationkey = n1.nationkey
/// JOIN orders o ON li.orderkey = o.orderkey
/// JOIN customer c ON o.custkey = c.custkey
/// JOIN nation n2 ON c.nationkey = n2.nationkey
/// GROUP BY n1.nationkey, n2.nationkey`.
///
/// Simplified: returns `HashMap<(supp_nationkey, cust_nationkey), sum_extprice>`.
pub fn q7_batch(
    suppliers: &[Supplier],
    items: &[Lineitem],
    orders: &[Order],
    customers: &[Customer],
) -> HashMap<(i64, i64), i64> {
    let mut result: HashMap<(i64, i64), i64> = HashMap::new();
    for li in items {
        let supp = suppliers.iter().find(|s| s.suppkey == li.suppkey);
        let order = orders.iter().find(|o| o.orderkey == li.orderkey);
        if let (Some(s), Some(o)) = (supp, order) {
            let cust = customers.iter().find(|c| c.custkey == o.custkey);
            if let Some(c) = cust {
                *result.entry((s.nationkey, c.nationkey)).or_insert(0) += li.extprice;
            }
        }
    }
    result
}

/// Evaluate Q8-style: `SELECT nation.nationkey, SUM(li.extprice * (1 - li.discount/1000))
/// FROM part JOIN lineitem ON part.partkey = li.suppkey
/// JOIN orders ON li.orderkey = orders.orderkey
/// JOIN customer ON orders.custkey = customer.custkey
/// JOIN nation ON customer.nationkey = nation.nationkey
/// WHERE part.p_type = 0
/// GROUP BY nation.nationkey`.
///
/// Simplified: filter on p_type == 0, group by nation.
pub fn q8_batch(
    parts: &[Part],
    items: &[Lineitem],
    orders: &[Order],
    customers: &[Customer],
    nations: &[Nation],
) -> HashMap<i64, i64> {
    let type0_suppkeys: std::collections::HashSet<i64> = parts
        .iter()
        .filter(|p| p.p_type == 0)
        .map(|p| p.partkey)
        .collect();

    let mut result: HashMap<i64, i64> = HashMap::new();
    for li in items {
        if !type0_suppkeys.contains(&li.suppkey) {
            continue;
        }
        let order = orders.iter().find(|o| o.orderkey == li.orderkey);
        if let Some(o) = order {
            let cust = customers.iter().find(|c| c.custkey == o.custkey);
            if let Some(c) = cust {
                let nation = nations.iter().find(|n| n.nationkey == c.nationkey);
                if let Some(n) = nation {
                    *result.entry(n.nationkey).or_insert(0) += li.extprice;
                }
            }
        }
    }
    result
}

/// Evaluate Q9-style: `SELECT s.nationkey, SUM(li.extprice - ps.supplycost * li.qty)
/// FROM supplier s JOIN lineitem li ON s.suppkey = li.suppkey
/// JOIN partsupp ps ON ps.suppkey = li.suppkey AND ps.partkey = li.orderkey
/// GROUP BY s.nationkey`.
///
/// Simplified: profit = extprice - supplycost × qty.
pub fn q9_batch(
    suppliers: &[Supplier],
    items: &[Lineitem],
    partsupp: &[PartSupp],
) -> HashMap<i64, i64> {
    let mut result: HashMap<i64, i64> = HashMap::new();
    for li in items {
        let supp = suppliers.iter().find(|s| s.suppkey == li.suppkey);
        let ps = partsupp
            .iter()
            .find(|p| p.suppkey == li.suppkey && p.partkey == li.orderkey);
        if let (Some(s), Some(p)) = (supp, ps) {
            let profit = li.extprice - p.supplycost * li.qty;
            *result.entry(s.nationkey).or_insert(0) += profit;
        }
    }
    result
}

/// Evaluate Q10-style: `SELECT c.custkey, SUM(li.extprice * (1000 - li.discount))
/// FROM customer c JOIN orders o ON c.custkey = o.custkey
/// JOIN lineitem li ON o.orderkey = li.orderkey
/// WHERE li.returnflag = 1
/// GROUP BY c.custkey`.
///
/// Returns `HashMap<custkey, revenue_from_returns>`.
pub fn q10_batch(
    customers: &[Customer],
    orders: &[Order],
    items: &[Lineitem],
) -> HashMap<i64, i64> {
    let mut result: HashMap<i64, i64> = HashMap::new();
    for li in items {
        if li.returnflag != 1 {
            continue;
        }
        let order = orders.iter().find(|o| o.orderkey == li.orderkey);
        if let Some(o) = order {
            let cust = customers.iter().find(|c| c.custkey == o.custkey);
            if let Some(c) = cust {
                let revenue = li.extprice * (1000 - li.discount);
                *result.entry(c.custkey).or_insert(0) += revenue;
            }
        }
    }
    result
}

/// Evaluate Q11-style: `SELECT ps.partkey, SUM(ps.supplycost * qty) AS value
/// FROM partsupp ps JOIN supplier s ON ps.suppkey = s.suppkey
/// WHERE s.nationkey = 1
/// GROUP BY ps.partkey
/// HAVING SUM(ps.supplycost * qty) > threshold`.
///
/// Returns `HashMap<partkey, value>` filtered by threshold.
pub fn q11_batch(
    partsupp: &[PartSupp],
    suppliers: &[Supplier],
    qty_per_partsupp: &HashMap<(i64, i64), i64>,
    threshold: i64,
) -> HashMap<i64, i64> {
    let nation1_supps: std::collections::HashSet<i64> = suppliers
        .iter()
        .filter(|s| s.nationkey == 1)
        .map(|s| s.suppkey)
        .collect();
    let mut result: HashMap<i64, i64> = HashMap::new();
    for ps in partsupp {
        if !nation1_supps.contains(&ps.suppkey) {
            continue;
        }
        let qty = qty_per_partsupp
            .get(&(ps.partkey, ps.suppkey))
            .copied()
            .unwrap_or(1);
        let val = ps.supplycost * qty;
        *result.entry(ps.partkey).or_insert(0) += val;
    }
    result.retain(|_, v| *v > threshold);
    result
}

/// Evaluate Q12-style: `SELECT li.returnflag, COUNT(*) FROM lineitem
/// WHERE li.discount > 50 GROUP BY li.returnflag`.
///
/// Returns `HashMap<returnflag, count>`.
pub fn q12_batch(items: &[Lineitem]) -> HashMap<i64, i64> {
    let mut result: HashMap<i64, i64> = HashMap::new();
    for li in items {
        if li.discount > 50 {
            *result.entry(li.returnflag).or_insert(0) += 1;
        }
    }
    result
}

/// Evaluate Q13-style: `SELECT o.custkey, COUNT(o.orderkey) AS c_count
/// FROM orders o GROUP BY o.custkey`.
///
/// Returns `HashMap<custkey, order_count>`.
pub fn q13_batch(orders: &[Order]) -> HashMap<i64, i64> {
    let mut result: HashMap<i64, i64> = HashMap::new();
    for o in orders {
        *result.entry(o.custkey).or_insert(0) += 1;
    }
    result
}

/// Evaluate Q14-style: `SELECT SUM(CASE WHEN p.p_type = 0 THEN li.extprice ELSE 0 END)
/// / SUM(li.extprice) AS promo_revenue FROM lineitem li JOIN part p ON li.suppkey = p.partkey`.
///
/// Simplified: returns `(sum_promo, sum_total)` for ratio computation.
pub fn q14_batch(items: &[Lineitem], parts: &[Part]) -> (i64, i64) {
    let promo_parts: std::collections::HashSet<i64> = parts
        .iter()
        .filter(|p| p.p_type == 0)
        .map(|p| p.partkey)
        .collect();
    let mut sum_promo = 0i64;
    let mut sum_total = 0i64;
    for li in items {
        sum_total += li.extprice;
        if promo_parts.contains(&li.suppkey) {
            sum_promo += li.extprice;
        }
    }
    (sum_promo, sum_total)
}

/// Evaluate Q15-style: `SELECT s.suppkey, SUM(li.extprice) AS revenue
/// FROM supplier s JOIN lineitem li ON s.suppkey = li.suppkey
/// GROUP BY s.suppkey` then find MAX(revenue).
///
/// Returns `(max_revenue, Vec<suppkey>)` for all suppliers with max revenue.
pub fn q15_batch(suppliers: &[Supplier], items: &[Lineitem]) -> (i64, Vec<i64>) {
    let mut revenue: HashMap<i64, i64> = HashMap::new();
    for li in items {
        *revenue.entry(li.suppkey).or_insert(0) += li.extprice;
    }
    let max_rev = revenue.values().copied().max().unwrap_or(0);
    let top_supps: Vec<i64> = suppliers
        .iter()
        .filter(|s| revenue.get(&s.suppkey).copied().unwrap_or(0) == max_rev)
        .map(|s| s.suppkey)
        .collect();
    (max_rev, top_supps)
}

/// Evaluate Q16-style: `SELECT p.size, COUNT(DISTINCT ps.suppkey) AS cnt
/// FROM part p JOIN partsupp ps ON p.partkey = ps.partkey
/// WHERE p.p_type != 1
/// GROUP BY p.size`.
///
/// Returns `HashMap<size, distinct_supplier_count>`.
pub fn q16_batch(parts: &[Part], partsupp: &[PartSupp]) -> HashMap<i64, i64> {
    let mut size_to_supps: HashMap<i64, std::collections::HashSet<i64>> = HashMap::new();
    for p in parts {
        if p.p_type == 1 {
            continue;
        }
        for ps in partsupp {
            if ps.partkey == p.partkey {
                size_to_supps.entry(p.size).or_default().insert(ps.suppkey);
            }
        }
    }
    size_to_supps
        .into_iter()
        .map(|(size, supps)| (size, supps.len() as i64))
        .collect()
}

/// Evaluate Q17-style: `SELECT SUM(li.extprice) / 7 AS avg_yearly
/// FROM lineitem li WHERE li.qty < (0.2 * AVG(qty over li.suppkey group))`.
///
/// Simplified: returns revenue from items with qty below 20% of group average.
pub fn q17_batch(items: &[Lineitem]) -> i64 {
    let mut supp_qty_sum: HashMap<i64, (i64, i64)> = HashMap::new(); // suppkey → (sum, count)
    for li in items {
        let e = supp_qty_sum.entry(li.suppkey).or_insert((0, 0));
        e.0 += li.qty;
        e.1 += 1;
    }
    let mut total = 0i64;
    for li in items {
        if let Some((sum, cnt)) = supp_qty_sum.get(&li.suppkey) {
            let avg = sum / cnt.max(&1);
            if li.qty * 5 < avg {
                // qty < 0.2 * avg  ↔  qty * 5 < avg
                total += li.extprice;
            }
        }
    }
    total / 7
}

/// Evaluate Q18-style: `SELECT o.custkey, COUNT(*) FROM orders o
/// JOIN lineitem li ON o.orderkey = li.orderkey
/// GROUP BY o.custkey
/// HAVING COUNT(*) > 3`.
///
/// Returns `HashMap<custkey, lineitem_count>` for high-volume customers.
pub fn q18_batch(orders: &[Order], items: &[Lineitem]) -> HashMap<i64, i64> {
    let mut order_count: HashMap<i64, i64> = HashMap::new();
    for li in items {
        *order_count.entry(li.orderkey).or_insert(0) += 1;
    }
    let mut result: HashMap<i64, i64> = HashMap::new();
    for o in orders {
        let cnt = order_count.get(&o.orderkey).copied().unwrap_or(0);
        if cnt > 3 {
            *result.entry(o.custkey).or_insert(0) += cnt;
        }
    }
    result
}

/// Evaluate Q19-style: `SELECT SUM(li.extprice * (1000 - li.discount))
/// FROM lineitem li JOIN part p ON li.suppkey = p.partkey
/// WHERE (p.p_type = 0 AND li.qty < 11)
///    OR (p.p_type = 1 AND li.qty < 20)`.
///
/// Returns the single aggregate value.
pub fn q19_batch(items: &[Lineitem], parts: &[Part]) -> i64 {
    let part_map: HashMap<i64, i64> = parts.iter().map(|p| (p.partkey, p.p_type)).collect();
    items
        .iter()
        .filter(|li| {
            let p_type = part_map.get(&li.suppkey).copied().unwrap_or(-1);
            (p_type == 0 && li.qty < 11) || (p_type == 1 && li.qty < 20)
        })
        .map(|li| li.extprice * (1000 - li.discount))
        .sum()
}

/// Evaluate Q20-style: `SELECT s.suppkey FROM supplier s
/// WHERE s.suppkey IN (SELECT ps.suppkey FROM partsupp ps
///   WHERE ps.supplycost < (SELECT 0.5 * SUM(li.qty) FROM lineitem li
///     WHERE li.suppkey = ps.suppkey))`.
///
/// Simplified: suppliers whose total supplied qty (in lineitem) exceeds 2×supplycost.
pub fn q20_batch(suppliers: &[Supplier], partsupp: &[PartSupp], items: &[Lineitem]) -> Vec<i64> {
    let mut supp_qty: HashMap<i64, i64> = HashMap::new();
    for li in items {
        *supp_qty.entry(li.suppkey).or_insert(0) += li.qty;
    }
    let mut result = Vec::new();
    for s in suppliers {
        let qty = supp_qty.get(&s.suppkey).copied().unwrap_or(0);
        let cost: i64 = partsupp
            .iter()
            .filter(|ps| ps.suppkey == s.suppkey)
            .map(|ps| ps.supplycost)
            .sum();
        if qty > 2 * cost {
            result.push(s.suppkey);
        }
    }
    result.sort_unstable();
    result
}

/// Evaluate Q21-style: `SELECT s.suppkey, COUNT(*) FROM supplier s
/// JOIN lineitem li ON s.suppkey = li.suppkey
/// WHERE li.returnflag = 2
/// GROUP BY s.suppkey
/// HAVING COUNT(*) > 0`.
///
/// Returns `HashMap<suppkey, return_count>`.
pub fn q21_batch(suppliers: &[Supplier], items: &[Lineitem]) -> HashMap<i64, i64> {
    let supp_set: std::collections::HashSet<i64> = suppliers.iter().map(|s| s.suppkey).collect();
    let mut result: HashMap<i64, i64> = HashMap::new();
    for li in items {
        if li.returnflag == 2 && supp_set.contains(&li.suppkey) {
            *result.entry(li.suppkey).or_insert(0) += 1;
        }
    }
    result
}

/// Evaluate Q22-style: `SELECT c.nationkey, COUNT(*), SUM(c.acctbal)
/// FROM customer c
/// WHERE c.acctbal > avg_acctbal_for_nation
/// GROUP BY c.nationkey`.
///
/// Simplified: customers with acctbal > national average.
/// Returns `HashMap<nationkey, (count, sum_acctbal)>`.
pub fn q22_batch(customers: &[Customer], acctbal: &HashMap<i64, i64>) -> HashMap<i64, (i64, i64)> {
    // Compute national average acctbal from the provided map.
    let mut nation_sum: HashMap<i64, (i64, i64)> = HashMap::new();
    for (ck, bal) in acctbal {
        if let Some(c) = customers.iter().find(|c| c.custkey == *ck) {
            let e = nation_sum.entry(c.nationkey).or_insert((0, 0));
            e.0 += bal;
            e.1 += 1;
        }
    }
    let nation_avg: HashMap<i64, i64> = nation_sum
        .iter()
        .map(|(n, (sum, cnt))| (*n, if *cnt > 0 { sum / cnt } else { 0 }))
        .collect();

    let mut result: HashMap<i64, (i64, i64)> = HashMap::new();
    for c in customers {
        let bal = acctbal.get(&c.custkey).copied().unwrap_or(0);
        let avg = nation_avg.get(&c.nationkey).copied().unwrap_or(0);
        if bal > avg {
            let e = result.entry(c.nationkey).or_insert((0, 0));
            e.0 += 1;
            e.1 += bal;
        }
    }
    result
}

// ─── IVM accumulator ─────────────────────────────────────────────────────────

/// Lightweight IVM accumulator: applies a closure to each row of a ZSet
/// delta and accumulates the results. This simulates the incremental
/// output of a query pipeline without full operator instantiation.
///
/// Returns the final accumulated `HashMap<group_key, aggregate>`.
/// Row delta type alias.
pub type DeltaRow = (Vec<u8>, Vec<u8>, i64);

/// Lightweight IVM accumulator: applies a closure to each row of a ZSet
/// delta and accumulates the results. This simulates the incremental
/// output of a query pipeline without full operator instantiation.
///
/// Returns the final accumulated `HashMap<group_key, aggregate>`.
pub fn ivm_accumulate<F>(deltas: &[Vec<DeltaRow>], extract: F) -> HashMap<i64, i64>
where
    F: Fn(&[u8], &[u8]) -> Option<(i64, i64)>,
{
    let mut state: HashMap<i64, i64> = HashMap::new();
    for delta in deltas {
        for (key, value, weight) in delta {
            if let Some((group, measure)) = extract(key, value) {
                *state.entry(group).or_insert(0) += measure * weight;
            }
        }
    }
    state.retain(|_, v| *v != 0);
    state
}
