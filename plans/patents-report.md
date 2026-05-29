# RockStream Patent Freedom-to-Operate Report

> **Status**: Internal engineering analysis, 2026-05-29. This is **not**
> a legal opinion. It is a structured, engineer-authored risk
> assessment intended to (a) document the patent landscape RockStream
> operates in, (b) flag the small set of patents that warrant ongoing
> attention, and (c) prescribe the engineering and documentation
> practices that minimise infringement risk. Before any commercial
> release or material outside investment, this report should be reviewed
> by qualified patent counsel.
>
> **Companion document**: [plans/prior-art.md](prior-art.md) catalogues
> the public technical literature and open-source implementations that
> establish prior art for every technique RockStream uses. References of
> the form `prior-art.md §N` point into that file.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Methodology and Scope](#2-methodology-and-scope)
3. [Highest-Risk Patent Family: Microsoft Differential / Timely Dataflow](#3-highest-risk-patent-family-microsoft-differential--timely-dataflow)
4. [DBSP / Z-Set Algebra](#4-dbsp--z-set-algebra)
5. [Snowflake Materialized-View and Pruning Patents](#5-snowflake-materialized-view-and-pruning-patents)
6. [Older / Expired IVM Patents](#6-older--expired-ivm-patents)
7. [Cloud-Native LSM-on-Object-Storage Patents](#7-cloud-native-lsm-on-object-storage-patents)
8. [Streaming Engines: Confluent, Databricks, Google, AWS](#8-streaming-engines-confluent-databricks-google-aws)
9. [Apache Iceberg / Delta / REST Catalog](#9-apache-iceberg--delta--rest-catalog)
10. [Postgres Wire Protocol](#10-postgres-wire-protocol)
11. [CRDT and Merge-Law Patents](#11-crdt-and-merge-law-patents)
12. [Coordinator / Lease-Quorum / Consensus](#12-coordinator--lease-quorum--consensus)
13. [Engineering Guidelines](#13-engineering-guidelines)
14. [Documentation and Attribution Requirements](#14-documentation-and-attribution-requirements)
15. [Ongoing FTO Process](#15-ongoing-fto-process)
16. [Open Questions for Counsel](#16-open-questions-for-counsel)

---

## 1. Executive Summary

RockStream sits in a saturated, well-published field. The IVM, streaming
SQL, lakehouse, and consensus building blocks RockStream depends on are
overwhelmingly:

- **Old enough to be outside any patent term** (relational IVM theory
  from 1986–2000; Paxos 1989; LSM 1996; consistent hashing 1997).
- **Published under permissive open licences with explicit patent
  grants** (Apache 2.0 covers Iceberg, Delta, Flink, RisingWave,
  SlateDB, etcd, FoundationDB, Beam).
- **Published as peer-reviewed papers and reference-implemented under
  MIT** (timely-dataflow, differential-dataflow, Feldera/DBSP).

After a survey of approximately 60 candidate patents across nine
technology areas, **a single patent family warrants active mitigation**:

> **Microsoft's "Differential Dataflow" / "Reachability-based
> Coordination for Cyclic Dataflow" family** — US9165035B2, US9832068B2,
> US10171284B2, and their continuations. Priority 2012-12-17. Expiry
> ≈2032-12-17. Inventor of record: Frank D. McSherry et al.

The mitigation strategy for this family is detailed in §3. In summary:

1. The 2012 CIDR paper and 2012-10 Microsoft Research tech report
   (prior-art.md §2.1, §2.2) pre-date the 2012-12-17 priority date and
   constitute documented prior art against the broadest claims.
2. The named inventor open-sourced functionally equivalent reference
   implementations under the MIT licence (timely-dataflow,
   differential-dataflow) which Microsoft has had over a decade to
   challenge and has not.
3. Materialize Inc., RisingWave Labs, and Feldera Inc. all build
   commercial products on this stack without challenge from Microsoft.
4. RockStream will document its frontier model as derived from the
   public Naiad SOSP'13 paper and the MIT-licensed reference, and will
   not copy the specific scheduling/compaction state-machine language
   used in the patent claims.

**No other identified patent presents material risk.** The Snowflake,
VMware, IBM, Oracle, Ant Group, NetApp, Amazon, and Google patents
reviewed either (a) read on specific competitor mechanisms RockStream
does not implement, (b) are routed through upstream Apache-2.0 licensed
dependencies (SlateDB, Iceberg, Beam), or (c) have already expired.

**Recommended posture**: ship freely against the current design. Adopt
the engineering and documentation rules in §13 and §14. Re-run the FTO
review (§15) before each minor release and before any commercial GA.

---

## 2. Methodology and Scope

### 2.1 What was searched

| Source | Queries |
|---|---|
| Google Patents | "differential dataflow", "incremental view maintenance", "materialized view incremental", "frontier antichain", "log-structured object storage", "Iceberg REST catalog", "CRDT", "stream processing exactly once", "consistent hashing virtual node", filtered by assignee for Snowflake Inc., Microsoft Technology Licensing LLC, Databricks Inc., Confluent Inc., Materialize Inc., Amazon, Oracle, IBM, VMware, NetApp, Ant Group. |
| USPTO PatentsView | Same query set, cross-checked. |
| EPO Espacenet | International family check on the top 12 patents. |
| Academic literature | ACM Digital Library, arXiv, DBLP — to identify dated public disclosures. |
| Open-source code | License files and patent grants on GitHub for the reference implementations RockStream uses or could use as defensive citations. |

### 2.2 Threat model

For each candidate patent we asked four questions:

1. **Claim scope**: do the independent claims plausibly read on the
   actual RockStream design as documented in
   [DESIGN.md](../DESIGN.md), [IVM.md](../IVM.md), and the crate
   layout?
2. **Status**: granted, pending, expired, abandoned, lapsed for
   non-payment? Term-adjusted expiry date?
3. **Prior art**: is there published prior art predating the priority
   date that would be material under §102 / §103 (US) or Art. 54 / 56
   EPC?
4. **Mitigation cost**: can RockStream avoid the claim by design
   without losing material capability?

A patent is classified **High** if (1) is yes, (2) is granted/in-term,
(3) is weak, and (4) is expensive. **Watch** if (1) and (2) are yes but
(3) is strong or (4) is cheap. **Low** otherwise.

### 2.3 What this report does not do

- It does not provide a legal opinion. Patent counsel must review
  before any GA release.
- It does not cover trademark, copyright, or trade-secret risk.
- It does not cover patents filed after 2025-12-01. The §15 ongoing
  process is the mechanism for catching newer filings.

---

## 3. Highest-Risk Patent Family: Microsoft Differential / Timely Dataflow

### 3.1 Patents in the family

| Patent | Title | Assignee | Inventors | Priority | Granted | Expiry (est.) |
|---|---|---|---|---|---|---|
| **US9165035B2** | "Differential dataflow" | Microsoft Technology Licensing LLC | F. McSherry, D. Murray, R. Isaacs, M. Isard | 2012-05-10 | 2015-10-20 | ≈2032-05-10 |
| **US9832068B2** | "Reachability-based coordination for cyclic dataflow" (parent) | Microsoft Technology Licensing LLC | F. McSherry, R. Isaacs, M. Isard, D. Murray | 2012-12-17 | 2017-11-28 | ≈2032-12-17 |
| **US10171284B2** | (continuation of US9832068) | Microsoft Technology Licensing LLC | same | 2012-12-17 | 2019-01-01 | ≈2032-12-17 |
| EP, WO, CA, JP family members | various | Microsoft | same | 2012 | various | various |

### 3.2 Claim scope summary

The independent claims of US10171284B2 (representative of the family)
cover:

1. A method of scheduling worker threads against partitions of data in
   a dataflow graph.
2. Maintaining a *replicated progress-tracking data structure* whose
   entries are **vertex–time pairs** (vertex, (epoch, iteration)).
3. Detecting that a vertex at a time can produce no more output by
   checking that the count of yet-to-be-processed pointstamps is zero
   under a defined partial order.
4. Computing the **minimal antichain** of in-flight pointstamps as the
   global frontier.
5. Compacting the progress structure by advancing frozen entries to
   their *least upper bound*.
6. Handling cyclic dataflow via an explicit *increment* vertex on each
   back-edge.

These read closely onto RockStream's documented frontier model in
DESIGN.md §6 ("Frontier Algebra") and §13 ("Epoch Coordination").

### 3.3 Prior art analysis

Two prior-art items pre-date the 2012-12-17 priority of the
coordination patent (US9832068B2 / US10171284B2):

| Prior-art item | Date | Material disclosure |
|---|---|---|
| McSherry, Murray, Isaacs, Isard, *"Differential Dataflow"*, CIDR 2012 (prior-art.md §2.1) | 2012-01-06 | Discloses the differential operator over Z-sets and the multi-dimensional time used to coordinate it. 11 months before priority. |
| Microsoft Research Tech Report MSR-TR-2012-105, *"Composable Incremental and Iterative Data-Parallel Computation with Naiad"* (prior-art.md §2.2) | 2012-10-09 | Discloses the coordination clock, frontier, antichain, and pointstamp scheme. 2 months before priority. |

The CIDR 2012 paper is a peer-reviewed, openly-published disclosure by
the same authors who later filed the patents, and it discloses the
core algebraic content of the family. The MSR-TR-2012-105 disclosure
is more specific to the coordination-clock claims and likewise pre-dates
the priority date.

For the broader US9165035B2 (priority 2012-05-10), the CIDR 2012 paper
itself (2012-01-06) is prior art.

**Subjective assessment** (engineer, not counsel): an IPR or EPO
opposition built on these prior-art items has a credible chance of
narrowing or invalidating the broadest claims. However, RockStream
should not rely on such invalidation; the patents are presumed valid
until challenged, and challenge is expensive.

### 3.4 Defensive shields

The five shields below substantially reduce real-world risk **without
relying on invalidation**:

1. **Inventor open-sourced the reference implementation under MIT.** F.
   McSherry, the lead named inventor, is the principal author of the
   `timely-dataflow` and `differential-dataflow` Rust crates, published
   on GitHub from 2014 under the MIT licence. The MIT licence contains
   no patent retention; under settled doctrines of patent exhaustion
   and implied licence (US) and *good faith* (most civil-law
   jurisdictions), public distribution of a working reference
   implementation by the inventor, under a permissive licence with no
   reservation, materially undermines later assertion against
   downstream users of that implementation or of independently-written
   equivalent implementations.
2. **Microsoft has not asserted in 10+ years.** US9165035B2 was granted
   2015-10-20. In the decade since, Microsoft has not asserted against
   any of: Materialize Inc., Feldera Inc., RisingWave Labs, the
   `timely-dataflow` project, or academic users. Non-assertion does
   not legally estop future assertion but is highly probative of
   actual risk.
3. **Established commercial users.** Materialize, Inc. (founded 2019,
   backed by Lightspeed, KP, GV) has shipped a production differential-
   dataflow product for 6+ years. Their continued operation is
   substantial evidence that Microsoft does not regard the patent as
   commercially worth asserting.
4. **Microsoft's Open Specification Promise / Open Invention Network
   membership.** Microsoft is a member of the Open Invention Network
   and has made multiple public commitments not to assert patents
   against open-source Linux-system software. RockStream is open
   source and would qualify under OIN's definition.
5. **Defensive prior-art publication.** RockStream itself, as a public
   open-source project documenting its frontier design in DESIGN.md
   and IVM.md, becomes prior art for any *new* Microsoft filings on
   the same techniques.

### 3.5 Mitigation rules for RockStream

Apply the following design and documentation rules:

1. **Cite Naiad SOSP'13 and DBSP VLDB'23 as the design references** for
   the frontier model in `DESIGN.md`, `IVM.md`, and any
   academic-style write-up. Do not derive the design narrative from
   the patent specification.
2. **Adopt DBSP's algebraic vocabulary** (Z-sets, streams,
   integrators, differentiators, lifted operators) for the
   user-visible contract. DBSP is CC-BY published mathematics; its
   vocabulary is not patent-encumbered. The Microsoft claims are
   written in operational language ("yet-to-be-processed count",
   "vertex-time pair", "increment vertex") that maps to a *specific
   scheduler implementation*, not to an algebraic specification.
3. **Avoid copying the specific operational language of the patent
   claims** into RockStream code, comments, or documentation. In
   particular, do not name internal data structures after the
   patent's terms-of-art (e.g. do not call the progress structure a
   "yet-to-be-processed table" or "vertex-time pair table"). Use
   neutral, DBSP/Naiad-derived names like "frontier", "antichain",
   and "progress message".
4. **Document the upstream provenance of every borrowed idea** with a
   citation in [plans/prior-art.md](prior-art.md). Where the idea is
   implemented similarly to `timely-dataflow`, cite the MIT-licensed
   crate explicitly; this both gives credit and establishes the
   defensive shield in §3.4(1).
5. **Do not import any code from Microsoft Research projects whose
   licence is not Apache 2.0, MIT, or BSD.** The `timely-dataflow` and
   `differential-dataflow` crates are acceptable upstream sources;
   any internal Microsoft project, codeplex archive, or
   research-licensed code is not.
6. **In any commercial branding, white papers, or marketing**, describe
   the system as "DBSP-based" or "Z-set IVM" rather than "differential
   dataflow", to reinforce the algebraic-rather-than-operational
   framing. (This is a marketing rule, not a technical one — the
   underlying mathematics is the same.)

### 3.6 Risk class: **Watch (medium)**

Active monitoring required; no immediate design change required beyond
the §3.5 documentation rules.

---

## 4. DBSP / Z-Set Algebra

### 4.1 Search result

A focused Google Patents and PatentsView search for filings naming
"DBSP", "Z-set", "Z set", "weighted multiset incremental", or the
named DBSP authors (Budiu, Chajed, Ryzhyk, Tannen) as inventors on a
database patent returned **no granted patent that reads on the DBSP
algebra**.

The DBSP paper itself (Budiu et al., arXiv:2203.16684, VLDB 2023) is
licensed CC-BY 4.0 (see arXiv licence record). The reference
implementation `feldera/feldera` was originally `Copyright 2021–2023
VMware, Inc.` and is now `Copyright 2023–2026 Feldera, Inc.`, released
under the **MIT licence** with no patent reservation.

### 4.2 VMware / Broadcom risk

VMware was the original employer of the DBSP authors. After the
Broadcom acquisition (2023-11), Broadcom has, in unrelated areas,
shown willingness to monetise the VMware patent portfolio. However:

- No DBSP-specific patent has been identified in the VMware portfolio.
- The DBSP paper was published 2022-03 on arXiv before any
  hypothetical 2022+ VMware filing could have priority, making any
  later filing on the DBSP algebra itself anticipated by VMware's own
  publication.
- The MIT-licensed `feldera` repository was originally VMware-owned;
  publication under MIT without patent reservation creates implied
  licence to downstream users.

### 4.3 Risk class: **Low**

No specific mitigation required. The §13 documentation rule (cite
DBSP paper and CC-BY licence in design docs) is sufficient.

---

## 5. Snowflake Materialized-View and Pruning Patents

Snowflake, Inc. holds ≈1,900 granted US patents. The relevant subset:

| Patent | Title | Priority | Risk Class |
|---|---|---|---|
| US11030186B2 | "Incremental refresh of a materialized view" (Cruanes) | 2018-10-26 | Watch |
| CA3035445C | "Incremental clustering maintenance of a table" (Cruanes) | 2016-09-02 | Low |
| US20230394009A1 | "Data pruning based on metadata" (Zukowski/Dageville) | 2014–2016 | Watch |
| US20230161735A1 | (related continuation) | 2014–2016 | Watch |
| CA3021963C | "Multi-cluster warehouse" | 2016-04-28 | Low |
| US9842152B2 | "Transparent discovery of semi-structured data schema" | 2014-02-19 | Low |

### 5.1 US11030186B2 — Incremental MV refresh

The claims describe a specific state-machine for refreshing an MV by
comparing a "snapshot table version" against a "view version", emitting
*compensation* and *application* deltas. This is similar in spirit to
RockStream's epoch-driven refresh but is described in proprietary
Snowflake terminology tied to micropartition versioning.

**Mitigation**: RockStream's `REFRESH MATERIALIZED VIEW` is driven by
the DBSP differentiator/integrator over Z-sets at a global frontier
boundary — a fundamentally different mechanism than Snowflake's
micropartition-version comparison. As long as RockStream's
documentation and source describe the mechanism in DBSP terms and not
in Snowflake terms ("micropartition", "snapshot table version",
"compensation delta"), the claim is not infringed.

**Rule**: do not adopt the terms "micropartition", "snapshot table
version", or "compensation delta" in RockStream code or docs.

### 5.2 Data-pruning patents

US20230394009A1 / US20230161735A1 cover storing per-file metadata and
using it to prune files at query time. This is a near-universal
lakehouse technique (Iceberg, Delta, Hudi all do it). RockStream's
shard-level statistics for pruning falls in the same category.

**Mitigation**: RockStream pruning is implemented inside the upstream
SlateDB and Iceberg layers, which are Apache 2.0 with patent grants
from their contributors. RockStream itself does not implement a novel
metadata-pruning algorithm. As long as we rely on Iceberg / SlateDB
pruning rather than reimplementing it, we are inside the Apache 2.0
patent grant of those upstream projects.

### 5.3 Risk class: **Watch (low)**

Active rule: do not copy Snowflake-specific terminology or describe
internal mechanisms in language that maps onto Snowflake's claim
elements. Otherwise, RockStream's mechanisms diverge sufficiently from
the Snowflake claims that infringement is unlikely.

---

## 6. Older / Expired IVM Patents

The following catalogue is included for completeness. None present
active risk; many are now expired and constitute prior art.

| Patent | Assignee / Inventor | Priority | Status | Notes |
|---|---|---|---|---|
| US7647298B2, EP2008206B1, US10268742B2, US8739118B2 | Microsoft / Adya | 2006–2007 | Granted; expires 2026–2027 | Entity Framework ORM mapping IVM. Narrow scope; not analytical IVM. Does not read on RockStream. |
| CN101385029B | Microsoft / Larson | 2006-02-15 | Granted; expires 2026 | Outer-join MV maintenance. Narrow algorithm choice; RockStream uses Larson/Zhou ICDE 2007 published algorithm, not the patent's. |
| US7953707B2 | IBM / Hamel | 2002 | **EXPIRED 2022** | "How to Roll a Join" async IVM. Now public domain prior art. |
| US9177275B2 | IBM / Nesamoney | 2003-05-27 | Granted; expires 2023 | Continuous heterogeneous enterprise view. Likely expired. |
| US9052908B2 | UC Regents | 2010-01-22 | Granted; expires 2030 | Reactive partial-update / spreadsheet IVM. Different domain; does not read on RockStream. |
| US9740741B2 | Hasso Plattner Institut | 2013-04-11 | Granted; expires 2033 | Aggregate caching with differential buffer. SAP HANA-specific differential buffer; not RockStream's design. |
| US10789242B2 | Microsoft | 2018-04-25 | Granted; expires 2038 | MVs on eventually-consistent stores using CDC offsets. Different model (CDC-driven vs DBSP-driven); does not read. |
| US20220171759A1 | Amazon | 2020-11-28 | Pending application | Schema-incompatibility detection for views. Narrow; not relevant. |
| US11514041B2 | Oracle | 2020-09-14 | Granted; expires 2040 | ML estimation of MV refresh duration. RockStream does not estimate refresh duration with ML. Easily avoided. |

### 6.1 Risk class: **Low**

No mitigation required beyond not reimplementing the specific narrow
techniques. The expired patents are useful as defensive prior art.

---

## 7. Cloud-Native LSM-on-Object-Storage Patents

| Patent | Assignee | Priority | Notes |
|---|---|---|---|
| US11675745B2 | VMware | 2020-11-13 | "Scalable I/O on LSM tree with cloud storage". Reads on VMware-internal vSAN-style implementation. |
| US11436102B2 | VMware | 2020-08-20 | "Log-structured formats for archived object storage". |
| CN110413592B | Alibaba | 2018-04-26 | LSM key-range compaction on OSD. |
| US10942852B1, US10885022B1 | Ant Group | 2019-09-12 | Log-structured storage with multi-tier object storage. |
| US12332864B2 | NetApp | 2021-04-20 | KV / filesystem integration. |

### 7.1 Mitigation

RockStream does **not** implement its own LSM-on-object-store. The
sole dependency is [SlateDB](https://github.com/slatedb/slatedb),
which is independently authored and released under the Apache 2.0
licence. Apache 2.0 §3 provides an explicit, irrevocable patent
licence grant from every contributor covering "patent claims
licensable by such Contributor that are necessarily infringed by
their Contribution(s) alone or by combination of their Contribution(s)
with the Work".

Any patent risk in the LSM-on-object-store space routes through
**SlateDB upstream**, not through RockStream. The appropriate
mitigation is to (a) track SlateDB releases, (b) raise concerns
upstream in the SlateDB community if a patent challenge emerges, and
(c) consider switching to an alternative (e.g. a tablet built directly
on object storage without LSM, or RocksDB on local disk with shipped
SSTables to object storage) if SlateDB is ever encumbered.

### 7.2 Risk class: **Low (routed upstream)**

---

## 8. Streaming Engines: Confluent, Databricks, Google, AWS

### 8.1 Findings

- **Confluent** (assignee searches): patents centred on Kafka cluster
  management, schema registry, ksqlDB query-rewrite optimisations.
  None read on RockStream's DBSP-based IVM. RockStream consumes Kafka
  via the public protocol (no Confluent-specific extensions).
- **Databricks**: patents on Delta Lake internals (column mapping,
  z-ordering, liquid clustering). RockStream uses Delta as a *sink*
  via the Apache-licensed `delta-rs` library; Apache 2.0 §3 grants
  the necessary patent licence.
- **Google**: Dataflow Model patents. The reference implementation
  Apache Beam is Apache 2.0 with explicit Google patent grant.
- **AWS**: Kinesis / Redshift patents are not relevant to RockStream's
  architecture; RockStream does not implement Kinesis-shard rebalancing
  or Redshift-specific MV refresh.

### 8.2 Risk class: **Low**

---

## 9. Apache Iceberg / Delta / REST Catalog

All implementations within the Apache 2.0 licence boundary. The Apache
2.0 patent grant explicitly covers RockStream's use of these specs as
sinks and of the REST Catalog protocol as a server surface.

**Risk class: Low.**

---

## 10. Postgres Wire Protocol

The PostgreSQL frontend/backend protocol is under the PostgreSQL
licence (BSD-style) and has been re-implemented by dozens of vendors
for 23 years without challenge.

**Risk class: Low.**

---

## 11. CRDT and Merge-Law Patents

A targeted search for CRDT-on-database patents identified a few
narrowly-scoped filings (e.g. Microsoft's US10579641B2 on CRDT
synchronization for collaborative editing, priority 2018-08-01). None
read on RockStream's algebraic-aggregate / merge-law column types,
which follow the openly-published INRIA 2011 CRDT taxonomy
(prior-art.md §10.1).

**Risk class: Low.**

---

## 12. Coordinator / Lease-Quorum / Consensus

Paxos (Lamport 1989) and Raft (Ongaro/Ousterhout 2014) are openly
published with no enforceable patents in the form RockStream uses.
etcd, FoundationDB, and TiKV provide Apache-2.0-licensed reference
implementations.

**Risk class: Low.**

---

## 13. Engineering Guidelines

The rules below are **binding on all RockStream contributions** going
forward. They should be reproduced in a short form in `CONTRIBUTING.md`
once this report is accepted.

### 13.1 Vocabulary discipline

- Use the **DBSP / Naiad academic vocabulary** for IVM and dataflow
  coordination: *frontier*, *antichain*, *Z-set*, *delta*, *epoch*,
  *integrator*, *differentiator*, *progress message*.
- **Avoid** vocabulary specific to a patent specification:
  - Microsoft coordination patents: "yet-to-be-processed count",
    "vertex-time pair table", "increment vertex" (use "back-edge
    coordinator" instead).
  - Snowflake MV patents: "micropartition", "snapshot table version",
    "compensation delta", "view version compare".
  - SAP HANA patents: "main store / delta store" pair (use "stable
    base / pending delta" instead).
- Cite the academic source for every borrowed concept in DESIGN.md and
  IVM.md.

### 13.2 Implementation discipline

- Frontier coordination, antichain compaction, and epoch advancement
  must be implemented from the DBSP / Naiad academic descriptions.
  Do not transcribe pseudocode or state-machine diagrams from any
  Microsoft, Snowflake, or other proprietary patent specification.
- Where the implementation is similar to `timely-dataflow` or
  `differential-dataflow`, a `// Inspired by timely-dataflow, MIT
  licence, F. McSherry et al.` comment at the top of the relevant
  module is sufficient and is encouraged. (This is one of the
  exceptions to the no-trivial-comments rule, because it carries
  legal weight.)
- LSM and object-storage layout is provided by SlateDB. Do not
  reimplement LSM in any RockStream crate.
- Iceberg / Delta sink implementation must use upstream Apache-2.0
  libraries (`iceberg-rust`, `delta-rs`). Do not reimplement file
  format internals.

### 13.3 Sink and source discipline

- Postgres wire protocol implementation should follow the published
  PGGD specification; cite the spec URL in the gateway crate's
  module-level rustdoc.
- Iceberg REST Catalog implementation should follow the published
  OpenAPI spec; cite the spec URL.

### 13.4 Forbidden code provenance

Do not import, copy, or paraphrase code from:

- Microsoft internal repositories (codeplex archives,
  research-licensed projects).
- Snowflake / Databricks / Confluent proprietary code, including
  decompiled binaries.
- Any source whose licence is "source-available" only (BSL pre-change
  date, SSPL, Confluent Community Licence) for production code paths.
  Reading such code for inspiration is permissible only if no code,
  algorithm pseudocode, or distinctive structural choice is carried
  across.

### 13.5 Marketing language

Public marketing and conference talks should describe RockStream as
"DBSP-based", "Z-set IVM", or "incremental view maintenance over
Apache Iceberg". Avoid "differential dataflow" as a *product
description* (the underlying mathematics is the same, but the term is
associated with the Microsoft patent family). Use "differential
dataflow" only when citing the academic literature.

---

## 14. Documentation and Attribution Requirements

The following documentation deliverables formalise the defensive
posture and should be kept current:

1. **[plans/prior-art.md](prior-art.md)** — the master prior-art
   catalogue. Updated whenever a new technique is added to
   RockStream.
2. **`DESIGN.md` — Prior-Art Section.** Add a short "Prior Art and
   Patent Posture" section to `DESIGN.md` that cites the academic
   references for the frontier model (Naiad SOSP'13), the IVM model
   (DBSP VLDB'23), and the storage substrate (SlateDB). One-paragraph
   addition; references prior-art.md for detail.
3. **Module-level rustdoc citations.** Each crate whose internals
   correspond to a patented-by-someone-else technique area carries a
   single rustdoc citation at the top of `lib.rs`:
   - `rockstream-ops`: cite DBSP (arXiv:2203.16684, CC-BY 4.0) and
     timely-dataflow (MIT, GitHub URL).
   - `rockstream-storage`: cite SlateDB (Apache 2.0) and the
     1996 O'Neil LSM paper.
   - `rockstream-gateway`: cite the PostgreSQL protocol spec.
   - `rockstream-control`: cite Raft (Ongaro/Ousterhout 2014) and etcd
     (Apache 2.0).
4. **`CONTRIBUTING.md` patent-hygiene checklist.** A short bullet list
   reproducing §13.1, §13.2, §13.4.
5. **Defensive publication.** As DESIGN.md and IVM.md are public
   open-source documents under the project's licence, they themselves
   become prior art against any future filing on RockStream-specific
   techniques. Make sure every novel mechanism (e.g. the specific
   bucket-migration state machine in DESIGN.md §10.2) is described in
   enough detail to be enabling under §112(a) / Art. 83 EPC.

---

## 15. Ongoing FTO Process

### 15.1 Cadence

| Trigger | Action | Owner |
|---|---|---|
| Each minor release (v0.x → v0.(x+1)) | Re-run patent search for assignees: Microsoft, Snowflake, Databricks, Confluent, Materialize, RisingWave, Feldera, Oracle, Amazon, Google, IBM, SAP, VMware/Broadcom. Diff against this report. | Tech lead |
| Each major release (v0.x → v1.0, v1 → v2) | Engage outside patent counsel for formal FTO opinion. | Founders / counsel |
| New architectural feature (anything that adds a §3-style mechanism) | Author short patent-rationale note in design ADR; cite prior art. | Feature author |
| Incoming patent assertion or notice letter | Stop all distribution of the named version; engage counsel immediately; do not respond directly without counsel review. | Founders / counsel |

### 15.2 Watchlist (assignees to monitor)

- Microsoft Technology Licensing LLC (largest source of dataflow
  patents).
- Snowflake Inc. (largest source of cloud-DB patents; ~1,900 grants).
- Databricks Inc. (Delta Lake, Photon, MosaicML).
- Confluent Inc. (Kafka and ksqlDB).
- Broadcom Inc. (post-VMware acquisition).
- SAP SE / Hasso Plattner Institut.
- Oracle Corporation.
- Amazon Technologies Inc.
- Google LLC.
- Materialize, Inc. (no patents currently, but monitor).
- Feldera, Inc. (no patents currently, but monitor).

### 15.3 Defensive Patent Strategy

Consider, before commercial GA:

- Joining the **Open Invention Network** (OIN) — provides cross-licence
  to the OIN portfolio for Linux-system software.
- Joining the **LOT Network** — provides protection against patent
  trolls (NPE assertions).
- Filing **defensive publications** on novel RockStream mechanisms via
  IP.com or the IBM Technical Disclosure Bulletin equivalent. This is
  cheaper than filing patents and establishes prior art against any
  future filing by competitors.
- Filing a small number of **defensive patents** on the most
  distinctive RockStream mechanisms (e.g. the specific bucket-migration
  protocol). Defensive patents do not prevent competitors from
  practising the technique but create cross-licence leverage if
  asserted against.

These are decisions for the founders and counsel; this report
recommends consideration but takes no position.

---

## 16. Open Questions for Counsel

When this report is reviewed by outside counsel, the following
questions should be addressed:

1. **US10171284B2 IPR posture.** Is the CIDR 2012 paper sufficient
   prior art, in counsel's judgement, to support an IPR petition on
   the broadest claims? Is it worth filing?
2. **Implied-licence doctrine.** Does counsel agree that the MIT
   release of `timely-dataflow` by the named inventor creates an
   implied patent licence in favour of downstream users implementing
   functionally equivalent systems?
3. **OIN / LOT membership timing.** What is the right release stage
   at which to join OIN and LOT?
4. **Snowflake risk.** Is US11030186B2 a meaningful concern given
   RockStream's algebraically-different refresh mechanism, or can it
   be safely deprioritised?
5. **Jurisdictional scope.** This report assumes US, EU, UK, and JP
   patent jurisdictions. Are there country-specific risks (e.g.
   China, India) that need separate treatment?
6. **Trademark.** "RockStream" trademark availability search is **out
   of scope** for this report and should be commissioned separately.

---

*End of report.*
