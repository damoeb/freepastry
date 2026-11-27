# Attack taxonomy coverage

Status on 27 November 2026 (`java25-upgrade`). Maps each attack class of the team's "Security Analysis and Advanced Attack Taxonomy of FreePastry" to what the code does today and where the fix sits on the resilience roadmap (waves A to D, see the slide deck in `docs/presentation/` and `docs/SECURITY-HANDOFF.md`). Certified mode is assumed unless a row says otherwise; plain mode trusts its network by design.

Decided 2026-11-27: the secure design (secure routing D1 with rate limiting C1–C3) also covers the adversarial test harness and metrics, diversity and churn, message integrity and replay, and implementation hygiene (dependency scanning, protocol state machines). Privacy and metadata (4.7) becomes wave E, after D.

Status: **Done** (fixed, with a test that failed before), **Partial** (some defence, a known gap remains), **Planned** (on the roadmap), **New** (not on the roadmap yet).

## 4.1 Identity and membership

| Attack | Status | Today | Gap and roadmap |
| --- | --- | --- | --- |
| 4.1.1 Sybil | Partial | Certified mode: the CA picks ids with SecureRandom, refuses requester-chosen ids without an operator flag and logs every issuance; a node is admitted per (id, address, epoch) after TLS | Per-requester issuance quota; detection of correlated participants (**New**). Plain mode: no defence by design |
| 4.1.2 Targeted NodeId concentration | Partial | Ids are random and CA-assigned, so a requester cannot pick a region | Density test on joins (C4); monitoring of the id-space distribution (**New**) |
| 4.1.3 Identity recycling | Partial | Epoch rules decide restarts; a claim never marks another node dead | Admission rate limits (C1–C3, in the secure design); reliability history (**New**) |

## 4.2 Routing table

| Attack | Status | Today | Gap and roadmap |
| --- | --- | --- | --- |
| 4.2.1 Routing table poisoning | Partial | Only verified handles enter the table (certificate, or a probe at the claimed address) | Slots go to the lowest RTT, so colluders near the victim win them: constrained routing table (D1) |
| 4.2.2 Prefix-based routing manipulation | Planned | Rerouting around dead next hops | Several candidates per slot, random choice, hysteresis, redundant routing (D1) |
| 4.2.3 Leaf-set manipulation | Partial | Leaf-set merge goes through admission; Scribe and PeerReview bind senders | Leaf-set broadcast and join senders bound to the authenticated hop (wave B, in progress); suppression of honest neighbours: density test (C4) |
| 4.2.4 Proximity manipulation | Partial | Ping nonce stops off-path forged pongs | Windowed RTT and per-peer HMAC on pongs (C5); autonomous-system and operator diversity in peer selection (**New**) |

## 4.3 Overlay topology

| Attack | Status | Today | Gap and roadmap |
| --- | --- | --- | --- |
| 4.3.1 Eclipse | Partial | Address takeover before TLS fixed (A4); join answers only from authenticated senders, every handle probed | One bootstrap is trusted: several bootstraps (C4); constrained table (D1) |
| 4.3.2 Partial partitioning | New | Leaf-set maintenance repairs local holes | Partition detection and merge of split rings |
| 4.3.3 Topology monoculture | New | None | Failure-domain-aware peer selection (with 4.2.4) |

## 4.4 Message routing and forwarding

| Attack | Status | Today | Gap and roadmap |
| --- | --- | --- | --- |
| 4.4.1 Selective message dropping | Planned | Hop-by-hop acknowledgements only | Routing failure test with a signed receipt from the root, redundant routing (D1) |
| 4.4.2 Message modification | Partial | TLS protects every link; Past content is hash-checked; Glacier manifests are owner-signed | A malicious hop can change a routed payload: optional end-to-end signatures for routed messages (**New**); signed Scribe topics (D2) |
| 4.4.3 Message replay | Partial | Pings carry nonces; PeerReview authenticators carry sequence numbers | Random request ids bound to the responder (B2); signed refresh with a counter (C6); a replay window for signed application operations (**New**) |
| 4.4.4 Routing state desynchronization | Partial | Ready state changes only on the selector thread; ArchUnit rules for futures | Periodic leaf-set and routing-table reconciliation checks (**New**) |

## 4.5 Object storage and applications

| Attack | Status | Today | Gap and roadmap |
| --- | --- | --- | --- |
| 4.5.1 Object poisoning | Partial | Content-hash objects verified on insert, cache and lookup; Glacier owner-signed manifests | Mutable Past content trusts one answer: quorum or signed content (C6); replica-set membership check (B3) |
| 4.5.2 Object suppression | Planned | Replication on the leaf set | Quorum lookups (C6), redundant routing (D1), availability monitoring (**New**) |
| 4.5.3 Stale object resurrection | Partial | Lease cap (365 days) on insert, refresh and replica fetch; a refresh never restores from the trash | Unsolicited replication responses still extend expirations (B3); signed expiration (C6) |

## 4.6 Availability and resource exhaustion

| Attack | Status | Today | Gap and roadmap |
| --- | --- | --- | --- |
| 4.6.1 RPC flooding | Partial | Bounded readers and tables, 10 s handshake deadline, 32 handshakes per IP, 64 datagrams per selection, fair TLS reads, rate-limited security logging | Per-peer and per-IP token buckets, socket and handshake budgets, fair scheduling, receive-memory budgets (C1–C3, part of the secure design) |
| 4.6.2 Join and maintenance abuse | Planned | Bounded liveness trackers | Admission and maintenance budgets (C1–C3) |
| 4.6.3 Churn manipulation | New | None | Churn-aware selection and conservative replacement (hysteresis in D1), availability history |

## 4.7 Privacy and metadata

| Attack | Status | Today | Gap and roadmap |
| --- | --- | --- | --- |
| 4.7.1 Lookup pattern inference | New | TLS hides payloads on links; every hop on a route sees the key by design | Wave E (privacy and metadata), after D |
| 4.7.2 Peer fingerprinting | New | Liveness pings and network-info probes go over UDP in clear | Wave E |
| 4.7.3 Cross-layer correlation | New | None | Wave E |

## 4.8 Implementation

| Attack | Status | Today | Gap and roadmap |
| --- | --- | --- | --- |
| 4.8.1 Parser differential | Partial | No Java serialization; typed raw serializers with read-side bounds; about 150 wire types round-trip tested; Jazzer fuzz targets | One implementation only, so differential testing needs a second reader or a spec (`docs/ProtocolSpec.txt` is outdated) (**New**) |
| 4.8.2 State machine inconsistencies | Partial | Fuzzing of the raw deserializers | Explicit protocol state machines and property-based tests (**New**) |
| 4.8.3 Java-specific risks | Partial | No remote input reaches an `Error` (source-scan test); bounded allocation on the attack paths found so far; single selector thread; exception containment per selector task (wave B, in progress) | Dependency vulnerability scanning in the build (**New**) |

## Sections 8 and 9: testing methodology and metrics

**New.** The direct simulator can host an adversarial test harness: honest and malicious nodes, controlled churn, failure domains, and the metrics of section 9 (delivery ratio, lookup success, routing-table and leaf-set diversity as Shannon entropy over failure domains, recovery time). No such harness exists yet. It is the natural yardstick for D1: measure before and after the constrained routing table.
