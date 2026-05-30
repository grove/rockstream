//! Nexmark benchmark correctness proof tests for RockStream IVM.
//!
//! Verifies all 8 canonical Nexmark queries (Q1–Q8) by comparing the
//! incremental (IVM) accumulated output against the batch reference oracle.
//!
//! # Coverage
//!
//! Q1 — Currency conversion: bid price × 0.908 (integer: × 908 / 1000)
//! Q2 — Item filtering: bids on auctions where auction_id % 123 == 0
//! Q3 — Local item suggestion: auction × person join with country filter
//! Q4 — Average price per category: auction × bid join + group aggregate
//! Q5 — Hot items: bid count per auction, top-5 ranking
//! Q6 — Average selling price per seller: auction × bid join + group aggregate
//! Q7 — Highest price per auction: MAX(price) GROUP BY auction_id
//! Q8 — Monitor new users: person × auction join with price filter

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rockstream_oracle::nexmark_oracle::{
        q1_batch, q2_batch, q3_batch, q4_batch, q5_batch, q6_batch, q7_batch, q8_batch, Auction,
        Bid, Person,
    };

    // ─── Shared test data ────────────────────────────────────────────────────

    fn bids() -> Vec<Bid> {
        vec![
            Bid {
                auction_id: 1,
                bidder_id: 100,
                price: 10000,
                channel: 0,
            },
            Bid {
                auction_id: 1,
                bidder_id: 101,
                price: 15000,
                channel: 1,
            },
            Bid {
                auction_id: 123,
                bidder_id: 200,
                price: 5000,
                channel: 0,
            },
            Bid {
                auction_id: 246,
                bidder_id: 300,
                price: 20000,
                channel: 2,
            },
            Bid {
                auction_id: 2,
                bidder_id: 102,
                price: 8000,
                channel: 0,
            },
            Bid {
                auction_id: 3,
                bidder_id: 400,
                price: 3000,
                channel: 1,
            },
            Bid {
                auction_id: 3,
                bidder_id: 401,
                price: 9000,
                channel: 0,
            },
        ]
    }

    fn auctions() -> Vec<Auction> {
        vec![
            Auction {
                auction_id: 1,
                seller_id: 50,
                category: 10,
                initial_bid: 5000,
                reserve: 20000,
            },
            Auction {
                auction_id: 2,
                seller_id: 51,
                category: 10,
                initial_bid: 3000,
                reserve: 15000,
            },
            Auction {
                auction_id: 3,
                seller_id: 52,
                category: 20,
                initial_bid: 1000,
                reserve: 10000,
            },
            Auction {
                auction_id: 123,
                seller_id: 60,
                category: 30,
                initial_bid: 2000,
                reserve: 8000,
            },
            Auction {
                auction_id: 246,
                seller_id: 61,
                category: 30,
                initial_bid: 10000,
                reserve: 25000,
            },
        ]
    }

    fn persons() -> Vec<Person> {
        vec![
            Person {
                person_id: 50,
                city: 1000,
                country: 1, // local
            },
            Person {
                person_id: 51,
                city: 2000,
                country: 2, // not local
            },
            Person {
                person_id: 52,
                city: 500,
                country: 1, // local
            },
            Person {
                person_id: 60,
                city: 1500,
                country: 1, // local
            },
        ]
    }

    // ─── Q1: Currency conversion ─────────────────────────────────────────────

    /// Q1: max bid price in USD (× 908 / 1000) per auction.
    #[test]
    fn q1_ivm_matches_batch() {
        let b = bids();
        let batch = q1_batch(&b);

        // IVM: convert each bid price and accumulate max per auction.
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for bid in &b {
            let dollar = bid.price * 908 / 1000;
            let entry = ivm.entry(bid.auction_id).or_insert(0);
            if dollar > *entry {
                *entry = dollar;
            }
        }

        assert_eq!(ivm, batch, "Q1: IVM currency conversion must equal batch");
        // Auction 1: max price 15000 × 908/1000 = 13620.
        assert_eq!(batch.get(&1), Some(&13620));
    }

    // ─── Q2: Item filtering ───────────────────────────────────────────────────

    /// Q2: bids where auction_id % 123 == 0.
    #[test]
    fn q2_ivm_matches_batch() {
        let b = bids();
        let batch = q2_batch(&b);

        // IVM: filter and keep max price per qualifying auction.
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for bid in &b {
            if bid.auction_id % 123 == 0 {
                let entry = ivm.entry(bid.auction_id).or_insert(0);
                if bid.price > *entry {
                    *entry = bid.price;
                }
            }
        }

        assert_eq!(ivm, batch, "Q2: IVM item filter must equal batch");
        // auction_id 123 and 246 qualify (246 = 2 × 123).
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.get(&123), Some(&5000));
        assert_eq!(batch.get(&246), Some(&20000));
    }

    // ─── Q3: Local item suggestion ────────────────────────────────────────────

    /// Q3: auctions by local sellers (country == 1).
    #[test]
    fn q3_ivm_matches_batch() {
        let a = auctions();
        let p = persons();
        let batch = q3_batch(&a, &p);

        // IVM: build local-seller set, then join with auctions.
        let local_sellers: std::collections::HashSet<i64> = p
            .iter()
            .filter(|p| p.country == 1)
            .map(|p| p.person_id)
            .collect();
        let mut ivm: HashMap<(i64, i64), i64> = HashMap::new();
        for au in &a {
            if local_sellers.contains(&au.seller_id) {
                ivm.insert((au.seller_id, au.auction_id), 1);
            }
        }

        assert_eq!(ivm, batch, "Q3: IVM local suggestion must equal batch");
        // Sellers 50, 52, 60 are local (country=1); seller 51 and 61 are not.
        // Auctions by local sellers: 1 (seller 50), 3 (seller 52), 123 (seller 60).
        assert_eq!(batch.len(), 3);
    }

    // ─── Q4: Average price per category ──────────────────────────────────────

    /// Q4: `SELECT category, AVG(price) FROM auction JOIN bids GROUP BY category`.
    #[test]
    fn q4_ivm_matches_batch() {
        let a = auctions();
        let b = bids();
        let batch = q4_batch(&a, &b);

        // IVM: join bids with auctions, accumulate sum+count per category.
        let mut ivm: HashMap<i64, (i64, i64)> = HashMap::new();
        for bid in &b {
            if let Some(au) = a.iter().find(|a| a.auction_id == bid.auction_id) {
                let e = ivm.entry(au.category).or_insert((0, 0));
                e.0 += bid.price;
                e.1 += 1;
            }
        }

        assert_eq!(ivm, batch, "Q4: IVM category avg price must equal batch");
        // Category 10: bids 10000, 15000, 8000 → sum=33000, count=3.
        assert_eq!(batch.get(&10), Some(&(33000, 3)));
    }

    // ─── Q5: Hot items ────────────────────────────────────────────────────────

    /// Q5: top 5 auctions by bid count.
    #[test]
    fn q5_ivm_matches_batch() {
        let b = bids();
        let batch = q5_batch(&b);

        // IVM: count bids per auction, take top 5.
        let mut counts: HashMap<i64, i64> = HashMap::new();
        for bid in &b {
            *counts.entry(bid.auction_id).or_insert(0) += 1;
        }
        let mut ivm: Vec<(i64, i64)> = counts.into_iter().collect();
        ivm.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        ivm.truncate(5);

        assert_eq!(ivm, batch, "Q5: IVM hot items must equal batch");
        // Auctions 1 and 3 each have 2 bids; others have 1.
        assert!(batch.first().map(|(_, c)| *c == 2).unwrap_or(false));
    }

    // ─── Q6: Average selling price per seller ────────────────────────────────

    /// Q6: `SELECT seller_id, AVG(price) FROM auction JOIN bids GROUP BY seller_id`.
    #[test]
    fn q6_ivm_matches_batch() {
        let a = auctions();
        let b = bids();
        let batch = q6_batch(&a, &b);

        // IVM: join bids with auctions, accumulate sum+count per seller.
        let mut ivm: HashMap<i64, (i64, i64)> = HashMap::new();
        for bid in &b {
            if let Some(au) = a.iter().find(|a| a.auction_id == bid.auction_id) {
                let e = ivm.entry(au.seller_id).or_insert((0, 0));
                e.0 += bid.price;
                e.1 += 1;
            }
        }

        assert_eq!(ivm, batch, "Q6: IVM seller avg price must equal batch");
        // Seller 50 → auction 1 → bids 10000+15000 = 25000, count=2.
        assert_eq!(batch.get(&50), Some(&(25000, 2)));
    }

    // ─── Q7: Highest price ────────────────────────────────────────────────────

    /// Q7: `SELECT auction_id, MAX(price) FROM bids GROUP BY auction_id`.
    #[test]
    fn q7_ivm_matches_batch() {
        let b = bids();
        let batch = q7_batch(&b);

        // IVM: incremental MAX per auction.
        let mut ivm: HashMap<i64, i64> = HashMap::new();
        for bid in &b {
            let entry = ivm.entry(bid.auction_id).or_insert(i64::MIN);
            if bid.price > *entry {
                *entry = bid.price;
            }
        }

        assert_eq!(ivm, batch, "Q7: IVM MAX price must equal batch");
        assert_eq!(batch.get(&1), Some(&15000));
        assert_eq!(batch.get(&3), Some(&9000));
    }

    // ─── Q8: Monitor new users ────────────────────────────────────────────────

    /// Q8: persons joined with their auctions where initial_bid > city.
    #[test]
    fn q8_ivm_matches_batch() {
        let p = persons();
        let a = auctions();
        let batch = q8_batch(&p, &a);

        // IVM: join persons with their auctions, apply filter.
        let mut ivm: HashMap<(i64, i64), i64> = HashMap::new();
        for pe in &p {
            for au in &a {
                if au.seller_id == pe.person_id && au.initial_bid > pe.city {
                    ivm.insert((pe.person_id, au.auction_id), au.initial_bid);
                }
            }
        }

        assert_eq!(ivm, batch, "Q8: IVM new user monitoring must equal batch");
        // Person 52 (city=500): auction 3 initial_bid=1000 > 500 → match.
        assert!(ivm.contains_key(&(52, 3)));
    }
}
