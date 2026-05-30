//! Nexmark benchmark correctness oracle for RockStream IVM.
//!
//! Provides schema helpers and batch reference evaluators for the 8 canonical
//! Nexmark queries (Q1–Q8). Each query is modelled as a composition of
//! Filter + Join + Aggregate operators. The batch oracle provides ground truth
//! for comparing against incremental (IVM) operator output.
//!
//! # Nexmark event streams
//!
//! - **Bid**: (auction_id, bidder, price, channel)
//! - **Auction**: (auction_id, seller, category, initial_bid, reserve)
//! - **Person**: (person_id, name, email_crypt, city, country)
//!
//! # Encoding
//!
//! All rows use compact binary encoding: key = 8-byte big-endian i64 primary
//! key; value = N × 8-byte big-endian i64 fields.

use std::collections::HashMap;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn enc(v: i64) -> [u8; 8] {
    v.to_be_bytes()
}

fn dec(b: &[u8]) -> i64 {
    if b.len() >= 8 {
        i64::from_be_bytes(b[..8].try_into().unwrap_or([0u8; 8]))
    } else {
        0
    }
}

fn encode_row(key: i64, fields: &[i64]) -> (Vec<u8>, Vec<u8>) {
    let k = enc(key).to_vec();
    let mut v = Vec::with_capacity(fields.len() * 8);
    for f in fields {
        v.extend_from_slice(&enc(*f));
    }
    (k, v)
}

fn decode_field(value: &[u8], idx: usize) -> i64 {
    let start = idx * 8;
    if value.len() >= start + 8 {
        dec(&value[start..])
    } else {
        0
    }
}

// ─── Nexmark event types ─────────────────────────────────────────────────────

/// A Nexmark bid event: (auction_id, bidder_id, price, channel).
#[derive(Clone, Debug)]
pub struct Bid {
    pub auction_id: i64,
    pub bidder_id: i64,
    pub price: i64,
    pub channel: i64, // encoded category (0=phone, 1=mail, …)
}

impl Bid {
    /// Encode as `(key=auction_id, value=[bidder_id, price, channel])`.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        encode_row(self.auction_id, &[self.bidder_id, self.price, self.channel])
    }

    /// Decode from `(key, value)`.
    pub fn decode(key: &[u8], value: &[u8]) -> Self {
        Bid {
            auction_id: dec(key),
            bidder_id: decode_field(value, 0),
            price: decode_field(value, 1),
            channel: decode_field(value, 2),
        }
    }
}

/// A Nexmark auction event: (auction_id, seller_id, category, initial_bid, reserve).
#[derive(Clone, Debug)]
pub struct Auction {
    pub auction_id: i64,
    pub seller_id: i64,
    pub category: i64,
    pub initial_bid: i64,
    pub reserve: i64,
}

impl Auction {
    /// Encode as `(key=auction_id, value=[seller_id, category, initial_bid, reserve])`.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        encode_row(
            self.auction_id,
            &[
                self.seller_id,
                self.category,
                self.initial_bid,
                self.reserve,
            ],
        )
    }

    /// Decode from `(key, value)`.
    pub fn decode(key: &[u8], value: &[u8]) -> Self {
        Auction {
            auction_id: dec(key),
            seller_id: decode_field(value, 0),
            category: decode_field(value, 1),
            initial_bid: decode_field(value, 2),
            reserve: decode_field(value, 3),
        }
    }
}

/// A Nexmark person event: (person_id, city, country).
#[derive(Clone, Debug)]
pub struct Person {
    pub person_id: i64,
    pub city: i64,    // encoded as integer
    pub country: i64, // encoded as integer
}

impl Person {
    /// Encode as `(key=person_id, value=[city, country])`.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        encode_row(self.person_id, &[self.city, self.country])
    }

    /// Decode from `(key, value)`.
    pub fn decode(key: &[u8], value: &[u8]) -> Self {
        Person {
            person_id: dec(key),
            city: decode_field(value, 0),
            country: decode_field(value, 1),
        }
    }
}

// ─── Nexmark batch reference evaluators ──────────────────────────────────────

/// Q1: Currency conversion.
///
/// `SELECT auction_id, bidder_id, price * 0.908 AS dollar_price FROM bids`
///
/// Simplified: multiply price by 908, divide by 1000 (integer arithmetic).
/// Returns `HashMap<auction_id, dollar_price>` for highest-price bid per auction.
pub fn q1_batch(bids: &[Bid]) -> HashMap<i64, i64> {
    let mut result: HashMap<i64, i64> = HashMap::new();
    for b in bids {
        let dollar = b.price * 908 / 1000;
        let entry = result.entry(b.auction_id).or_insert(0);
        if dollar > *entry {
            *entry = dollar;
        }
    }
    result
}

/// Q2: Item filtering.
///
/// `SELECT auction_id, price FROM bids WHERE auction_id % 123 = 0`
///
/// Returns `HashMap<auction_id, max_price>` for qualifying auctions.
pub fn q2_batch(bids: &[Bid]) -> HashMap<i64, i64> {
    let mut result: HashMap<i64, i64> = HashMap::new();
    for b in bids {
        if b.auction_id % 123 == 0 {
            let entry = result.entry(b.auction_id).or_insert(0);
            if b.price > *entry {
                *entry = b.price;
            }
        }
    }
    result
}

/// Q3: Local item suggestion.
///
/// `SELECT p.person_id, a.auction_id FROM auction a JOIN person p
///  ON a.seller_id = p.person_id WHERE p.country = 1`
///
/// Returns `HashMap<(seller_id, auction_id), 1>` for sellers from country=1.
pub fn q3_batch(auctions: &[Auction], persons: &[Person]) -> HashMap<(i64, i64), i64> {
    let local_sellers: std::collections::HashSet<i64> = persons
        .iter()
        .filter(|p| p.country == 1)
        .map(|p| p.person_id)
        .collect();

    let mut result: HashMap<(i64, i64), i64> = HashMap::new();
    for a in auctions {
        if local_sellers.contains(&a.seller_id) {
            result.insert((a.seller_id, a.auction_id), 1);
        }
    }
    result
}

/// Q4: Average price for a category.
///
/// `SELECT a.category, AVG(b.price) FROM auction a JOIN bids b
///  ON a.auction_id = b.auction_id GROUP BY a.category`
///
/// Returns `HashMap<category, (sum_price, count)>` for division to get AVG.
pub fn q4_batch(auctions: &[Auction], bids: &[Bid]) -> HashMap<i64, (i64, i64)> {
    let mut result: HashMap<i64, (i64, i64)> = HashMap::new();
    for b in bids {
        let auction = auctions.iter().find(|a| a.auction_id == b.auction_id);
        if let Some(a) = auction {
            let e = result.entry(a.category).or_insert((0, 0));
            e.0 += b.price;
            e.1 += 1;
        }
    }
    result
}

/// Q5: Hot items (most bid-upon auctions in a window).
///
/// Simplified (no window): `SELECT auction_id, COUNT(*) AS bid_count
///  FROM bids GROUP BY auction_id ORDER BY bid_count DESC LIMIT 5`
///
/// Returns the top 5 auctions by bid count as `Vec<(auction_id, bid_count)>`.
pub fn q5_batch(bids: &[Bid]) -> Vec<(i64, i64)> {
    let mut counts: HashMap<i64, i64> = HashMap::new();
    for b in bids {
        *counts.entry(b.auction_id).or_insert(0) += 1;
    }
    let mut top: Vec<(i64, i64)> = counts.into_iter().collect();
    top.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    top.truncate(5);
    top
}

/// Q6: Average selling price per seller.
///
/// `SELECT a.seller_id, AVG(b.price) FROM auction a JOIN bids b
///  ON a.auction_id = b.auction_id GROUP BY a.seller_id`
///
/// Returns `HashMap<seller_id, (sum_price, count)>`.
pub fn q6_batch(auctions: &[Auction], bids: &[Bid]) -> HashMap<i64, (i64, i64)> {
    let mut result: HashMap<i64, (i64, i64)> = HashMap::new();
    for b in bids {
        let auction = auctions.iter().find(|a| a.auction_id == b.auction_id);
        if let Some(a) = auction {
            let e = result.entry(a.seller_id).or_insert((0, 0));
            e.0 += b.price;
            e.1 += 1;
        }
    }
    result
}

/// Q7: Highest price bids.
///
/// `SELECT auction_id, MAX(price) AS max_price FROM bids GROUP BY auction_id`
///
/// Returns `HashMap<auction_id, max_price>`.
pub fn q7_batch(bids: &[Bid]) -> HashMap<i64, i64> {
    let mut result: HashMap<i64, i64> = HashMap::new();
    for b in bids {
        let entry = result.entry(b.auction_id).or_insert(i64::MIN);
        if b.price > *entry {
            *entry = b.price;
        }
    }
    result
}

/// Q8: Monitor new users who open auctions.
///
/// `SELECT p.person_id, a.auction_id FROM person p JOIN auction a
///  ON p.person_id = a.seller_id WHERE a.initial_bid > p.city`
///
/// Simplified: joins on seller_id = person_id, filters where initial_bid > city.
/// Returns `HashMap<(person_id, auction_id), initial_bid>`.
pub fn q8_batch(persons: &[Person], auctions: &[Auction]) -> HashMap<(i64, i64), i64> {
    let mut result: HashMap<(i64, i64), i64> = HashMap::new();
    for p in persons {
        for a in auctions {
            if a.seller_id == p.person_id && a.initial_bid > p.city {
                result.insert((p.person_id, a.auction_id), a.initial_bid);
            }
        }
    }
    result
}

// ─── IVM simulation ───────────────────────────────────────────────────────────

/// Simulate IVM accumulation for a Nexmark query by applying a projection +
/// filter closure to each batch delta and accumulating the aggregate.
///
/// `extract(key, value)` returns `Some((group_key, measure))` to include a
/// row, or `None` to skip. The measure is multiplied by the row's weight.
///
/// Returns `HashMap<group_key, aggregate_value>` after processing all deltas.
/// Row delta type alias.
pub type DeltaRow = (Vec<u8>, Vec<u8>, i64);

/// Simulate IVM accumulation for a Nexmark query by applying a projection +
/// filter closure to each batch delta and accumulating the aggregate.
///
/// `extract(key, value)` returns `Some((group_key, measure))` to include a
/// row, or `None` to skip. The measure is multiplied by the row's weight.
///
/// Returns `HashMap<group_key, aggregate_value>` after processing all deltas.
pub fn nexmark_ivm_accumulate<F>(deltas: &[Vec<DeltaRow>], extract: F) -> HashMap<i64, i64>
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

/// Simulate a Nexmark inner-join IVM:  for each bid delta, look up the
/// matching auction in the accumulated auction state, and apply the
/// combine closure to produce an output row.
///
/// Returns `HashMap<output_key, output_value>` accumulated over all deltas.
pub fn nexmark_join_accumulate<JoinF, CombineF>(
    bids: &[Bid],
    auctions: &[Auction],
    join_fn: JoinF,
    combine: CombineF,
) -> HashMap<i64, i64>
where
    JoinF: Fn(i64, &Bid) -> i64,                // extract join key from bid
    CombineF: Fn(&Bid, &Auction) -> (i64, i64), // (output_group, output_measure)
{
    let mut result: HashMap<i64, i64> = HashMap::new();
    for b in bids {
        let key = join_fn(b.auction_id, b);
        let auction = auctions.iter().find(|a| a.auction_id == key);
        if let Some(a) = auction {
            let (group, measure) = combine(b, a);
            *result.entry(group).or_insert(0) += measure;
        }
    }
    result
}
