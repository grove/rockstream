# Prior Art for RockStream

> **Status**: Research compiled 2026-05-29. Companion document to
> [plans/patents-report.md](patents-report.md). This file catalogues the
> public, dated, technical disclosures that establish prior art for every
> technique RockStream implements. Each entry is intended to be citable in
> a defensive publication, an IPR petition, or a freedom-to-operate (FTO)
> opinion.
>
> **Scope**: only material published before, or independently of, any
> patent that might be asserted against RockStream. The catalogue is
> ordered by RockStream subsystem and, within each subsystem, by
> publication date.
>
> **How to read this**: each item lists (i) the technique it describes,
> (ii) its public disclosure (paper, code, talk, RFC), (iii) its date and
> licence, and (iv) which RockStream subsystem relies on it. Patents that
> may read on the same technique are cross-referenced to
> `patents-report.md` and discussed there — not here.

---

## Table of Contents

1. [Incremental View Maintenance (IVM) — Core Theory](#1-incremental-view-maintenance-ivm--core-theory)
2. [Differential Dataflow, Timely Dataflow, and Frontier Coordination](#2-differential-dataflow-timely-dataflow-and-frontier-coordination)
3. [DBSP and Z-Set Algebra](#3-dbsp-and-z-set-algebra)
4. [Materialized Views in Relational Systems](#4-materialized-views-in-relational-systems)
5. [Streaming SQL and Continuous Queries](#5-streaming-sql-and-continuous-queries)
6. [Log-Structured Merge (LSM) Storage on Object Storage](#6-log-structured-merge-lsm-storage-on-object-storage)
7. [Shard / Tablet Splitting, Rebalancing, and Virtual Buckets](#7-shard--tablet-splitting-rebalancing-and-virtual-buckets)
8. [Postgres Wire Protocol Compatibility](#8-postgres-wire-protocol-compatibility)
9. [Apache Iceberg, Delta Lake, and the Iceberg REST Catalog](#9-apache-iceberg-delta-lake-and-the-iceberg-rest-catalog)
10. [CRDTs and Commutative-Replicated State](#10-crdts-and-commutative-replicated-state)
11. [Deterministic Simulation Testing](#11-deterministic-simulation-testing)
12. [Exactly-Once Streaming, Watermarks, and Late Data](#12-exactly-once-streaming-watermarks-and-late-data)
13. [Self-Adjusting Computation and Reactive Programming](#13-self-adjusting-computation-and-reactive-programming)
14. [Summary: Defensive Posture by Subsystem](#14-summary-defensive-posture-by-subsystem)

---

## 1. Incremental View Maintenance (IVM) — Core Theory

The academic study of IVM substantially predates the modern patent record.
The techniques RockStream uses — delta queries, count/sum maintenance,
overrides for outer joins and aggregates, recursive view maintenance —
were all openly published in the 1980s and 1990s.

| # | Disclosure | Date | Relevance |
|---|---|---|---|
| 1.1 | Blakeley, Larson, Tompa, *"Efficiently Updating Materialized Views"*, ACM SIGMOD 1986. | 1986-06 | First formal treatment of incrementally maintaining SPJ views; introduces the differential-based approach. |
| 1.2 | Gupta, Mumick (eds.), *"Materialized Views: Techniques, Implementations, and Applications"*, MIT Press, ISBN 0-262-57122-6. | 1999 | Authoritative survey gathering ~15 years of prior IVM publications, including count-based maintenance (Mumick/Quass/Mumick 1997), DRed for recursive views (Gupta/Mumick/Subrahmanian 1993), and outer-join maintenance. |
| 1.3 | Mumick, Quass, Mumick, *"Maintenance of data cubes and summary tables in a warehouse"*, ACM SIGMOD 1997. | 1997-05 | The "count column" trick for aggregate maintenance. Used by virtually every IVM system since, including pg_trickle and Feldera. |
| 1.4 | Gupta, Mumick, Subrahmanian, *"Maintaining views incrementally"*, ACM SIGMOD 1993. | 1993-05 | DRed algorithm for incremental maintenance of recursive Datalog views. Reference for RockStream's `WITH RECURSIVE` handling ([IVM.md](../IVM.md) §11). |
| 1.5 | Salem, Beyer, Cochrane, Lindsay, *"How to Roll a Join: Asynchronous Incremental View Maintenance"*, ACM SIGMOD 2000. | 2000-05 | Cited as prior art inside IBM patent US7953707B2; the public paper itself is the prior art that disproves novelty for the asynchronous-join family of patents. |
| 1.6 | Larson, Zhou, *"Efficient Maintenance of Materialized Outer-Join Views"*, ICDE 2007. | 2007-04 | Reference for RockStream's outer-join state policy (DESIGN.md §6 hardening pass). |
| 1.7 | Koch, *"Incremental query evaluation in a ring of databases"*, ACM PODS 2010. | 2010-06 | Algebraic foundation: every relational query can be incrementally maintained over a commutative ring. Direct predecessor of DBSP. |
| 1.8 | Koch et al., *"DBToaster: higher-order delta processing for dynamic, frequently fresh views"*, VLDB Journal 2014. | 2014 | Open-source DBToaster compiler; demonstrates higher-order delta calculus (the delta of a delta), which is the algorithmic core used by pg_trickle and DBSP and which RockStream inherits via those references. |

**Disposition**: every technique RockStream uses from this list is older
than 20 years (the maximum patent term in every major jurisdiction) at the
earliest claim date of any patent that could plausibly read on it. The
1986/1993/1997/1999/2010 publications are therefore unimpeachable prior
art.

---

## 2. Differential Dataflow, Timely Dataflow, and Frontier Coordination

This is the most patent-sensitive area for RockStream. The frontier model,
antichain semantics, vertex-time coordination clock, and partial-order
compaction are all features Microsoft patented (see
[patents-report.md](patents-report.md) §3). The public prior art and
the open-source MIT-licensed reference implementation are RockStream's
primary defences.

| # | Disclosure | Date | Licence | Relevance |
|---|---|---|---|---|
| 2.1 | McSherry, Murray, Isaacs, Isard, *"Differential Dataflow"*, CIDR 2012. | 2012-01-06 | Public conference paper | First peer-reviewed publication of differential dataflow. Pre-dates the priority date of Microsoft's coordination patent US9832068 (priority 2012-12-17) by 11 months. |
| 2.2 | McSherry, Isaacs, Isard, *"Composable Incremental and Iterative Data-Parallel Computation with Naiad"*, Microsoft Research Tech Report MSR-TR-2012-105. | 2012-10-09 | Public tech report | Discloses the coordination clock and frontier-based scheduling that the Microsoft patents later claim. Pre-dates the 2012-12-17 priority. |
| 2.3 | Murray, McSherry, Isaacs, Isard, Barham, Abadi, *"Naiad: A Timely Dataflow System"*, SOSP 2013. | 2013-11 | Public conference paper | Authoritative published description of timely dataflow, frontiers, antichains, pointstamps. Cited as a non-patent reference inside US10171284B2 itself. |
| 2.4 | [TimelyDataflow/timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow). | 2014-onwards | **MIT licence** | Reference implementation authored by McSherry (the named inventor on the Microsoft patents) and shipped under MIT licence with no patent retention. Materialize Inc., RisingWave, and many others build production systems on top of it without incident. |
| 2.5 | [TimelyDataflow/differential-dataflow](https://github.com/TimelyDataflow/differential-dataflow). | 2014-onwards | **MIT licence** | Reference implementation of differential dataflow. As above: MIT, no patent reservation, used by Materialize and others in production. |
| 2.6 | Abadi, McSherry, Plotkin, *"Foundations of Differential Dataflow"*, FoSSaCS 2015. | 2015-04 | Public conference paper | Formal semantics that further entrenches the public-domain status of the underlying calculus. |
| 2.7 | McSherry et al., *"Scalability! But at what COST?"*, HotOS 2015. | 2015-05 | Public conference paper | Independent academic discussion of single-machine timely dataflow performance — reinforces that the underlying ideas are public. |

**Disposition**: RockStream's frontier model and epoch coordination are
not derived from inspection of any Microsoft patent; they are derived
from (a) the published Naiad/CIDR/SOSP papers, and (b) the MIT-licensed
timely/differential reference implementations. The 2012 CIDR paper is
specifically prior art to the 2012-12-17 priority of US9832068 (the
parent of US10171284B2) — see §3 of the patents report for the legal
analysis.

---

## 3. DBSP and Z-Set Algebra

DBSP is the algebraic framework RockStream uses for its IVM correctness
contract (see [IVM.md](../IVM.md) §1).

| # | Disclosure | Date | Licence | Relevance |
|---|---|---|---|---|
| 3.1 | Budiu, Chajed, McSherry, Ryzhyk, Tannen, *"DBSP: Automatic Incremental View Maintenance for Rich Query Languages"*, arXiv:2203.16684, later published at VLDB 2023. | 2022-03-30 (arXiv); 2023-08 (VLDB) | **CC-BY 4.0** | The defining DBSP paper. Explicitly licensed CC-BY 4.0 on arXiv; freely usable. |
| 3.2 | Z-set / weighted multiset algebra. | 1990s | Public mathematical concept | Z-sets (signed multisets with integer weights) are a textbook construction in abstract algebra and category theory, long predating any database use. |
| 3.3 | [feldera/feldera](https://github.com/feldera/feldera). | 2023-onwards | **MIT licence** | Reference DBSP implementation. Originally `Copyright 2021–2023 VMware, Inc.`, re-released under MIT by Feldera, Inc. The `dbsp` crate on crates.io is MIT-licensed with no patent reservation; commercial reuse is unrestricted. |

**Disposition**: DBSP is open mathematics published under CC-BY with an
MIT-licensed reference implementation. No DBSP-specific patent has been
identified (see patents report §4). The algebraic vocabulary (Z-sets,
streams, lifted operators, integrators, differentiators) is not
patent-able as such under §101 / Article 52 EPC subject-matter rules.

---

## 4. Materialized Views in Relational Systems

RockStream's `CREATE MATERIALIZED VIEW`, `REFRESH`, and snapshot
semantics are direct descendants of long-established relational features.

| # | Disclosure | Date | Relevance |
|---|---|---|---|
| 4.1 | Oracle 8i materialized views (query rewrite). | 1999 | First commercial materialized-view system. Documented in Oracle 8i Tuning Release 8.1.5 (now archived). |
| 4.2 | IBM Db2 Materialized Query Tables (MQTs). | ~2001 | Db2 v7+ documentation. |
| 4.3 | SQL Server 2000 Indexed Views. | 2000 | Microsoft documentation; the indexed-view feature is the canonical Microsoft answer to materialized views and is the public technique that any later Microsoft patent must distinguish over. |
| 4.4 | PostgreSQL 9.3 `CREATE MATERIALIZED VIEW` and 9.4 `REFRESH MATERIALIZED VIEW CONCURRENTLY`. | 2013, 2014 | Open-source; the syntax RockStream copies for surface-level compatibility. |
| 4.5 | Date, *"The Relational Database Dictionary"*, O'Reilly 2006, ISBN 978-1-4493-9115-7, p. 59. | 2006 | Textbook definition of "materialization" and "snapshot" — establishes the public, non-novel status of the concept. |

**Disposition**: the `CREATE MATERIALIZED VIEW` syntax and refresh
semantics are 25+ years old in commercial use and are well outside any
enforceable patent.

---

## 5. Streaming SQL and Continuous Queries

RockStream's positioning as a streaming-SQL system follows a long line
of academic and commercial systems.

| # | Disclosure | Date | Relevance |
|---|---|---|---|
| 5.1 | Babu, Widom, *"Continuous Queries over Data Streams"*, SIGMOD Record 2001. | 2001-09 | Foundational STREAM/CQL paper from Stanford. |
| 5.2 | Arasu, Babcock, Babu, Cieslewicz, Datar, Ito, Motwani, Srivastava, Widom, *"STREAM: The Stanford Data Stream Management System"*, technical report. | 2003 | Foundational continuous-query system. |
| 5.3 | Apache Flink (originally Stratosphere, TU Berlin). | 2010-onwards | Open-source streaming SQL engine; Apache 2.0 licensed with patent grant. |
| 5.4 | Apache Spark Structured Streaming. | 2016 | Apache 2.0 licensed with patent grant. |
| 5.5 | Apache Kafka Streams. | 2016 | Apache 2.0. |
| 5.6 | RisingWave, [github.com/risingwavelabs/risingwave](https://github.com/risingwavelabs/risingwave). | 2021-onwards | **Apache 2.0** — includes explicit patent grant (Apache 2.0 §3). RisingWave's existence as an open-source streaming SQL system is itself a defensive shield: any patent that would block RockStream would also block RisingWave. |
| 5.7 | Materialize Inc., production system. | 2019-onwards | Commercial product built on MIT-licensed timely/differential dataflow. |

**Disposition**: streaming SQL is a saturated, well-published prior-art
field. The Apache 2.0 licences on Flink, Spark, Kafka, and RisingWave
provide explicit patent grants from major corporate contributors (IBM,
Cloudera, Confluent, Databricks, Alibaba, AWS) covering the techniques
they use.

---

## 6. Log-Structured Merge (LSM) Storage on Object Storage

RockStream depends on SlateDB; SlateDB is independently developed and
licensed.

| # | Disclosure | Date | Licence | Relevance |
|---|---|---|---|---|
| 6.1 | O'Neil, Cheng, Gawlick, O'Neil, *"The Log-Structured Merge-Tree (LSM-Tree)"*, Acta Informatica 33 (1996). | 1996 | Public publication | Foundational LSM publication. 30 years old; well outside any patent term. |
| 6.2 | LevelDB (Google). | 2011 | **BSD-3-Clause** | Reference LSM implementation, openly published. |
| 6.3 | RocksDB (Facebook/Meta). | 2012-onwards | **GPLv2 + Apache 2.0** dual-licensed | Production LSM with explicit Apache 2.0 patent grant from Meta. |
| 6.4 | [slatedb/slatedb](https://github.com/slatedb/slatedb). | 2024-onwards | **Apache 2.0** | The LSM RockStream uses. Apache 2.0 includes an explicit patent licence grant (§3). Authored by an independent team; no Microsoft, Oracle, or Snowflake authorship that would create a patent encumbrance. |

**Disposition**: SlateDB is upstream code that RockStream consumes
under Apache 2.0. Apache 2.0 §3 provides an irrevocable patent licence
from every contributor, covering "patent claims licensable by such
Contributor that are necessarily infringed by their Contribution(s)
alone or by combination of their Contribution(s) with the Work". This
covers the contributors' use of LSM-on-object-store techniques.

The independent VMware patents on LSM-over-object-storage
(US11675745B2, US11436102B2) cover specific VMware internal mechanisms
(vSAN/CNS-style virtual-disk LSMs); they do not read on the SlateDB
public design, which uses a different write-path and object-naming
scheme.

---

## 7. Shard / Tablet Splitting, Rebalancing, and Virtual Buckets

| # | Disclosure | Date | Relevance |
|---|---|---|---|
| 7.1 | Chang et al., *"Bigtable: A Distributed Storage System for Structured Data"*, OSDI 2006. | 2006-11 | Public publication of tablet-based sharding and online splitting. |
| 7.2 | DeCandia et al., *"Dynamo: Amazon's Highly Available Key-value Store"*, SOSP 2007. | 2007-10 | Consistent hashing with virtual nodes — the prior-art lineage for RockStream's virtual buckets ([DESIGN.md](../DESIGN.md) §7.1, §10.2). |
| 7.3 | Karger et al., *"Consistent Hashing and Random Trees: Distributed Caching Protocols for Relieving Hot Spots on the World Wide Web"*, STOC 1997. | 1997-05 | Original consistent-hashing paper. 28 years old. |
| 7.4 | Thaler, Ravishankar, *"A Name-Based Mapping Scheme for Rendezvous"*, University of Michigan TR CSE-TR-316-96. | 1996 | Rendezvous (HRW) hashing. RockStream uses rendezvous hashing for virtual bucket → physical shard mapping. |
| 7.5 | Apache Cassandra documentation, virtual nodes ("vnodes"). | 2012-onwards | Apache 2.0; openly documented use of virtual buckets for online rebalancing. |
| 7.6 | CockroachDB ranges (Raft-replicated key ranges with splitting). | 2015-onwards | **Business Source Licence → Apache 2.0** after 3 years; design openly published in CockroachLabs blog posts and the *"CockroachDB: The Resilient Geo-Distributed SQL Database"* SIGMOD 2020 paper. |

**Disposition**: virtual-bucket sharding, online splits, rendezvous
hashing, and the bucket-migration state machine all derive from
publicly published academic and open-source work spanning 1996–2020.

---

## 8. Postgres Wire Protocol Compatibility

| # | Disclosure | Date | Relevance |
|---|---|---|---|
| 8.1 | PostgreSQL Frontend/Backend Protocol specification, protocol versions 2.0 (1998) and 3.0 (2003). | 1998, 2003 | Open documentation under the PostgreSQL Licence (BSD-like). The protocol is openly published and widely re-implemented (CockroachDB, Amazon Redshift, YugabyteDB, Materialize, RisingWave, Greenplum, …). |
| 8.2 | PostgreSQL Global Development Group, *"PostgreSQL: The World's Most Advanced Open Source Relational Database"*, source code. | 1996-onwards | PostgreSQL Licence (BSD-style); no patent encumbrance. |
| 8.3 | Multiple independent pgwire re-implementations: `pgwire` (Rust), `postgres-protocol` (Rust), `wal-listener`, CockroachDB's `pgwire` package, Materialize's pgwire layer. | 2015-onwards | Demonstrates the protocol is universally re-implemented without patent disputes. |

**Disposition**: the Postgres wire protocol is a 23-year-old openly
published interface, re-implemented by dozens of vendors and projects
without challenge. There is no plausible patent risk in implementing it.

---

## 9. Apache Iceberg, Delta Lake, and the Iceberg REST Catalog

| # | Disclosure | Date | Licence | Relevance |
|---|---|---|---|---|
| 9.1 | [apache/iceberg](https://github.com/apache/iceberg). | 2018-onwards | **Apache 2.0** | Includes explicit patent grant from Netflix, Apple, AWS, Tabular, etc. The Iceberg v2 spec is openly published. |
| 9.2 | Iceberg REST Catalog OpenAPI specification. | 2022-onwards | Apache 2.0 | Open spec; implemented by Polaris, Unity Catalog, Gravitino, Tabular. |
| 9.3 | [delta-io/delta](https://github.com/delta-io/delta). | 2019-onwards | **Apache 2.0** | Includes patent grant from Databricks and other contributors. |
| 9.4 | DuckLake spec, [ducklake.select](https://ducklake.select/). | 2025 | **MIT licence** | DuckDB Labs' lakehouse catalog format, openly published. |

**Disposition**: implementing Iceberg v2 and Delta as sinks, and
exposing the Iceberg REST Catalog endpoint, sits entirely inside the
Apache 2.0 patent grant from the contributors of those specs. No
freedom-to-operate concern.

---

## 10. CRDTs and Commutative-Replicated State

RockStream's algebraic-aggregate / CRDT-column work (DESIGN.md
"Algebraic Aggregates and CRDTs" section) follows the established CRDT
literature.

| # | Disclosure | Date | Relevance |
|---|---|---|---|
| 10.1 | Shapiro, Preguiça, Baquero, Zawirski, *"A comprehensive study of Convergent and Commutative Replicated Data Types"*, INRIA RR-7506. | 2011-01 | The foundational CRDT survey defining G-Counter, PN-Counter, OR-Set, LWW-Register, etc. |
| 10.2 | Shapiro et al., *"Conflict-free Replicated Data Types"*, SSS 2011. | 2011-10 | Peer-reviewed publication of the same. |
| 10.3 | Riak (Basho) open-source CRDT implementations. | 2013-onwards | **Apache 2.0** |
| 10.4 | Yjs, Automerge, and other open-source CRDT libraries. | 2014-onwards | MIT / Apache 2.0 |
| 10.5 | Hellerstein, Conway, *"Keeping CALM: When Distributed Consistency is Easy"*, CACM 2020. | 2020 | Open publication of the CALM theorem that DESIGN.md §8.4 cites for the epoch-commit invariant. |

**Disposition**: CRDTs are 15-year-old peer-reviewed academic
constructions, widely implemented under permissive licences. No patent
risk.

---

## 11. Deterministic Simulation Testing

RockStream's testing strategy (DESIGN.md §17) borrows from FoundationDB
and TigerBeetle.

| # | Disclosure | Date | Relevance |
|---|---|---|---|
| 11.1 | FoundationDB simulation testing — Zhou, *"Testing Distributed Systems w/ Deterministic Simulation"*, Strange Loop 2014 talk; Apple/FoundationDB engineering blog posts. | 2014-onwards | Publicly described approach, openly demonstrated. |
| 11.2 | [apple/foundationdb](https://github.com/apple/foundationdb). | 2018-onwards (open source) | **Apache 2.0** | The simulator code is open-sourced under Apache 2.0 with a patent grant from Apple. |
| 11.3 | TigerBeetle simulator (VOPR). | 2021-onwards | **Apache 2.0** | Source-available simulator and discussion of paired assertions, fault model, and liveness checks. |
| 11.4 | Hypothesis, QuickCheck, and the wider property-based-testing literature. | 1999-onwards | Open. |

**Disposition**: deterministic simulation testing is a published,
openly-implemented engineering practice with explicit Apache 2.0 patent
grants from the principal implementers (Apple, TigerBeetle).

---

## 12. Exactly-Once Streaming, Watermarks, and Late Data

| # | Disclosure | Date | Relevance |
|---|---|---|---|
| 12.1 | Akidau, Bradshaw, Chambers, Chernyak, Fernández-Moctezuma, Lax, McVeety, Mills, Perry, Schmidt, Whittle, *"The Dataflow Model"*, VLDB 2015. | 2015-08 | Defines watermarks, windowing, triggers, and late-data handling as published by Google. |
| 12.2 | Apache Beam (originating from Google Cloud Dataflow SDK). | 2016-onwards | **Apache 2.0** | Open implementation of the Dataflow Model paper with explicit patent grant from Google. |
| 12.3 | Apache Flink documentation on event-time, watermarks, and exactly-once via two-phase commit sinks. | 2014-onwards | Apache 2.0. |
| 12.4 | Carbone et al., *"Lightweight Asynchronous Snapshots for Distributed Dataflows"*, arXiv:1506.08603. | 2015 | The Chandy-Lamport-style snapshot algorithm used by Flink; built on Chandy & Lamport, *"Distributed Snapshots: Determining Global States of Distributed Systems"*, ACM TOCS 1985 (40 years old). |

**Disposition**: watermarks and exactly-once semantics are openly
published. The Apache 2.0 grants from Google and the Apache Flink
contributors cover the practical implementations.

---

## 13. Self-Adjusting Computation and Reactive Programming

| # | Disclosure | Date | Relevance |
|---|---|---|---|
| 14.1 | Acar, *"Self-Adjusting Computation"*, PhD thesis, CMU 2005. | 2005 | Cited as non-patent prior art inside Microsoft's US10171284B2 — establishing it as recognised prior art for the broader incremental-computation field. |
| 14.2 | Alvaro et al., *"Consistency Analysis in Bloom: a CALM and Collected Approach"*, CIDR 2011. | 2011-01 | Cited as non-patent prior art inside US10171284B2; foundation for the CALM-based epoch-commit invariant RockStream uses. |

---

## 14. Summary: Defensive Posture by Subsystem

| RockStream Subsystem | Primary Prior Art | Primary Open Reference Implementation | Patent Risk Class (see patents-report.md) |
|---|---|---|---|
| IVM theory (delta queries, count maintenance, DRed) | §1 (1986–1999 publications) | pg_trickle, DBToaster | **None** — well outside any patent term |
| DBSP / Z-set algebra | §3 (arXiv 2022 CC-BY) | feldera (MIT) | **None** — open mathematics + MIT impl |
| Frontier / antichain coordination | §2 (CIDR 2012, SOSP 2013) | timely-dataflow (MIT) | **Watch** — US10171284B2 expires 2032-12-17; mitigation via prior-art and MIT-licensed reference implementation |
| Materialized view syntax | §4 (Oracle 1999, Postgres 2013) | PostgreSQL | **None** |
| Streaming SQL | §5 (STREAM 2001, Flink 2010) | Flink, RisingWave (Apache 2.0 + patent grant) | **None** |
| LSM on object storage | §6 (1996 + Apache 2.0 SlateDB) | SlateDB (Apache 2.0) | **None** — Apache 2.0 grant covers it |
| Virtual buckets / online splits | §7 (Dynamo 2007, Cassandra vnodes) | Cassandra, CockroachDB | **None** |
| Postgres wire protocol | §8 (1998 spec) | postgres, many | **None** |
| Iceberg / Delta sinks | §9 (Apache 2.0 specs) | apache/iceberg, delta-io/delta | **None** |
| CRDT column types | §10 (INRIA 2011) | Riak, Yjs, Automerge | **None** |
| Deterministic simulation | §11 (FoundationDB, TigerBeetle) | foundationdb (Apache 2.0) | **None** |
| Watermarks / exactly-once | §12 (Dataflow Model 2015) | Apache Beam (Apache 2.0) | **None** |

The single area of meaningful patent risk is the frontier-coordination
machinery, which is addressed in detail in
[plans/patents-report.md](patents-report.md) §3.
