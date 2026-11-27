# FreePastry security handoff

2026-11-27

Every finding of the 2026-11-05 security review is fixed on `java25-upgrade`: critical and high on 2026-11-05, medium and low on 2026-11-19. A second, resilience-focused analysis (2026-11-23: overlay attacks, denial of service, applications and trust) added waves A to D; waves A and B (apart from Past and Glacier response binding) and phases P0b, P2 and P3 of the secure design are fixed below. The plan for the rest: `docs/security/SECURE-DESIGN.md` (approved 2026-11-27), coverage of the team's attack taxonomy: `docs/security/ATTACK-COVERAGE.md`, overview for readers: the slide deck in `docs/presentation/`.

## Where things stand

| Item | State |
| --- | --- |
| Branch | `java25-upgrade` (the user calls it "develop"), pushed, PR #1 on GitHub `damoeb/freepastry` |
| Default build | `./mvnw -B clean verify`: BUILD SUCCESS at `0407ff49`, 1527 tests, 0 failures, 1 skipped (a POSIX-only test) |
| Integration build | `./mvnw -B clean verify -Pintegration`: BUILD SUCCESS, 11 tests, about 20 min. It runs only the integration-tagged tests, so run both builds |
| Java | `export JAVA_HOME=$HOME/.sdkman/candidates/java/25.0.5.0+9-amzn PATH=$JAVA_HOME/bin:$PATH` before `./mvnw` |
| Other branches | `master`, `upgdate`, `wip-ep-snapshot` (old, user-made); agent worktrees under `.claude/worktrees/` while work is in flight |
| Dependency scan | Every build writes a CycloneDX SBOM per module; OSV-Scanner on all of them at `0407ff49`: no known vulnerabilities. `-Psecurity-scan` (OWASP dependency-check) needs `NVD_API_KEY` |
| Local notes (not versioned) | `.git/q4/HANDOFF.md` (overall brief) and `.git/q4/security-review-2026-11-05.md` (full review with file and line references) |

Tests have a default JUnit timeout (120 s, 1200 s with `-Pintegration`) and print a thread dump when they hit it.

## Fixed in the security review

Each fix has a test that fails on `bf507352` (the tip before the review) and passes after.

| Finding | Commit | What changed |
| --- | --- | --- |
| One TCP "nodes request" killed the node (ConnectivityProbeApplication cast) | `4dee51b2` | Plain socket handles count as directly contactable |
| Truncated liveness ping killed the node | `210307a1` | Reader throws IOException; new Jazzer target `LivenessFuzzTest` |
| Join request row count out of range; forged "accepted" join to a ready node | `321ac910` | Reader bounds the count; accepted requests ignored unless joining |
| Glacier bloom filter hash parameters unchecked | `0a3cd6f3` | Reader requires each parameter in 1..bits |
| Accept with no pending connection threw NPE | `3bb1a830` | Null accept is ignored |
| Any unchecked exception on the selector thread destroyed the Environment | `b8bc80aa` | A bad datagram is dropped, a bad socket is closed; Errors still propagate |
| Certified mode: a UDP datagram passed for an authenticated previous hop | `c91f2654` | Messages go up only if they came over TLS with the sender's certificate; app datagram requests go over TLS |
| Certified mode: forged datagram marked a trusted neighbour dead | `46e20438` | Only a certificate changes an authenticated node's binding |
| Source routes with a repeated hop looped forever | `bf63296e` | Rejected when read |
| Past replication fetch stored a reply under the id it claimed | `2dc5532a` | Stored only under the requested id, after checkInsert |
| Content-hash content was never hashed | `e2685f2e` | Verified on insert, cache and lookup. API change: subclasses implement `computeContentHash(IdFactory)` |
| FileTransfer allocated whatever size the peer announced | `c0605016` | New `p2p_filetransfer_max_*` params bound sizes; protocol errors close the socket |
| Open relay: source routes and NetworkInfo probes reached any IP:port (W5, Medium) | `99c98192` | A hop asks its `SourceRouteForwardStrategy` for datagrams too, not only sockets; with no strategy it relays nothing. Socket nodes install `AddressPolicyForwardStrategy` (no wildcard, multicast or the node itself; loopback and link-local only if the node has such an address) plus the old liveness check for sockets. A NetworkInfo probe request may name only the requester's own IP; `ConnectivityProbeApplication` applies the same address policy. Remaining: a hop still relays to any other routable address, such as a non-Pastry host on the LAN, because it cannot tell peers apart |
| Unbounded tables: spoofed identities and routes grew `IdentityLayerCore.bindings` and the liveness trackers until OOM; a forged pong marked a route alive | `71d10ec4` | Both tables are LRU-bounded (new params `pastry_socket_identity_max_bindings`, `pastry_socket_liveness_max_trackers`, default 10000); bindings with pending messages and, in certified mode, bindings confirmed by a certificate (`BindStrategy.shouldKeepBinding`, new) are never forgotten; the dead-forever set is bounded too. A pong counts only if it echoes an outstanding ping of that route. API change: `LivenessConfig` gains `maxTrackers` |
| Big-message preallocation (`BigMessageReader`) | `ba70378d` | The buffer grows with the bytes received (8 KB start, doubling); a per-node budget `pastry_socket_writer_max_big_msg_in_flight` (default 64 MB) closes sockets that would exceed it. API change: `PriorityConfig` has a fourth component `maxBigMsgInFlight` |
| CA tool and key stores: empty CA password, `-p` in `ps`, blank node key password, 0644 files | `b1e8465d` | `CertificateAuthorityTool` refuses an empty password and reads it from `FREEPASTRY_CA_PASSWORD` or the console; `-p` is refused. `-store` node stores get a password from `FREEPASTRY_NODE_PASSWORD` or the console. `SSLTransportLayer` has a constructor taking the key password, which `CertifiedSocketPastryNodeFactory` passes. Key store files are written 0600 (`CertificateAuthorityTool.newPrivateOutputStream`). API change: `CertificateAuthorityTool -p` removed; `DistTutorial` needs the node store password |
| GC refresh accepted any expiration and restored trashed objects | `c4deb99c`, `ee0bd4f0` | A refresh, an insert and a replica fetched from a peer are all capped to now + `p2p_past_gc_max_lease` (default 365 days); a refresh never restores from the trash. Remaining: authorizing a refresh needs signed content |
| Glacier default "signature" was the key's own bytes: forged manifests, fragment squatting, endless lifetime | `c3cb6528` | Owner-signed manifests: the storage id derives from the owner's public key and the object's name, the manifest carries both and a SHA256withRSA signature; every receiving path verifies; only the owner can extend. API change: `GlacierDefaultPolicy` needs a `KeyPair` and the `IdFactory`, `GlacierPolicy.storageId()` is new, Glacier ids are per owner; the stub is `InsecureGlacierPolicy` in the test sources |
| Certified admission checked the id only (C3): an insider could advertise a trusted node's id at its own address | `6db04059` | A handle is admitted only if that exact (id, address, epoch) presented the id's certificate over TLS; any other handle is probed at its own address first. `LeafSet.merge` no longer bypasses admission. API change: `PastryNode.identityResolved(NodeHandle, boolean)`, generic `DeferredDeliveries` |
| Join-window trust: a joining node adopted any accepted JoinRequest | `6db04059` | Certified mode adopts an answer only from an authenticated sender, and every handle in it passes admission (address probe) before it enters the leaf set. Plain mode still trusts the answer: plain mode is for trusted networks only (README, ARCHITECTURE) |
| Probe reflection: forged ids triggered TLS probes to attacker-chosen addresses | `6db04059` | One probe per address at a time, 5 s back-off after a failure, at most 32 probes in flight (`CertifiedSocketPastryNodeFactory` constants) |
| Handshake slowloris and unbounded UDP drain (W9) | `44225fd1` | Incoming sockets must finish the handshake within `transport_wire_handshake_timeout_millis` (10 s); at most `transport_wire_max_handshaking_sockets_per_prefix` (32) per /24 or /64 prefix and 256 in total (since P2, which also added accept and datagram token buckets, separate socket pools and a bounded TLS handshake executor); at most `transport_wire_max_datagrams_per_read` (64) datagrams per selection. API: `P2PSocket.handshakeCompleted()`; code using the wire layer without a priority layer must call it |
| A forged PONG with a guessed timestamp marked a route alive | `5ee5348a` | PING and PONG carry a random 64-bit nonce; a PONG counts only if it echoes an outstanding ping's nonce. API change: `LivenessTransportLayer.pong(..., senderNonce, ...)` |
| Node certificates lacked extensions; the node's own leaf was a trust anchor; TLS version from JDK defaults | `42ce5682` | Node certs: basicConstraints CA:false, keyUsage, extendedKeyUsage serverAuth+clientAuth, random 128-bit serials. Trust store holds only the CA (`CertifiedSocketPastryNodeFactory.trustStoreOf`). TLS 1.3 only |
| `IdentityTransportLayerImpl.verify` threw RuntimeException on a malformed signature; predictable PeerReview temp file | `75c5ff1e` | A malformed signature is `SIGNATURE_BAD`; temp histories live in a private 0700 directory |
| Maven wrapper without checksum | `f8ffb0b7` | `distributionSha256Sum` pinned |
| `user.params` in the working directory merged over defaults; `nat_handler_class` initialized before its type check | `04c03ea1` | A params file is read only when the caller names it; the class is type-checked before initialization |

Behaviour change in certified mode: a node restarting at the same address is recognised only once it reconnects over TLS, which it does when it rejoins.

## Fixed in the resilience waves (2026-11-23 to 2026-11-27)

Each fix has a test that fails on the code before it, reproducing the attack (forged message, spoofed handle, malicious header, flood).

| Finding | Commit | What changed |
| --- | --- | --- |
| One forged Glacier message (a response type flagged as a request) or an out-of-range sync offset threw `Error` and destroyed the Environment, stopping every node in the JVM | `7009a331`, `18a3f9c7`, `98df9bc5`, `fd6c0b51` | Dropped and logged; offsets and fragment ids bounded; a source-scan test forbids `new Error(` in main code |
| A burst of tiny TLS frames overflowed the selector stack (`StackOverflowError` after about 1,600 messages); same on the write side | `b2610fb0`, `94ccc302` | Bounded loops, at most `transport_ssl_max_read_callbacks_per_select` (64) callbacks per socket per selector turn |
| Spoofed identity headers filled `SocketNodeHandleFactory`'s cache until out of memory (both modes) | `111cc9a6` | Weak canonicalisation map |
| Certified insider claimed an honest node's address before TLS; the victim rebound it and marked the honest node dead forever (eclipse) | `cb44a779` | An incoming connection proves the id only; only a connection this node dialled proves an address; a claim never marks a node dead |
| A datagram naming a dead node revived it; datagrams and incoming headers marked routes alive | `878c87f0` | Only a connection this node opened to a certified handle marks its route alive |
| Leaf-set broadcasts, consistent-join messages and PNS leaf-set responses that name another node as sender took effect (forged leases, two roots for one key) | `5f45de58` | In certified mode the named sender must be the authenticated connection (`PastryNode.acceptsNamedSender`); plain mode unchanged |
| Scribe trusted the `source` inside messages: forged acks, unsubscribes and publishes; unbounded topics, children, pending subscribes, anycast walks | `5882a2f9`, `735bdd85` | Source must be the authenticated previous hop; acks only for open subscribes; bounds. New `Application.deliver(id, msg, previousHop)` |
| PeerReview stored any certificate a peer returned (rogue CA, colluder's certificate for an honest id, expired) and replaced valid ones; unbounded certificate and pending-authenticator stores | `75a1fc52`, `80f1701a` | CA and subject check, no replacement of a valid certificate, bounded stores |
| CA tool: `java.util.Random` ids, requester-chosen ids, no issuance log, v1 CA certificate | `93f620f7` | SecureRandom; `-cn` only with `-allow-chosen-id`; 0600 issuance log; v3 CA certificate, `-reissue-ca` keeps existing node certificates valid |
| A node set ready off the selector thread when an app registered while the lease changed | `af1d50b8` | State changes only on the selector thread |
| A `RuntimeException` in a selector task or timer task destroyed the Environment | `d64f7c8e` | Contained per task; only `VirtualMachineError`, `LinkageError` or a broken `select()` destroy |
| After the TLS handshake a peer close never reached the reader | `94ccc302` | End of stream and resets reach reader and writer |
| Certified nodes relayed source routes for anyone | `46f5f3fe` | Off unless `pastry_socket_relay_source_routes_certified` |
| Storage and parameter files were world-readable | `9630d7ec` | `PrivateFiles`: 0600 files, 0700 directories |
| Remote input could flood the log | `e3d0d70d` | Rate-limited security log lines |
| Half-open sockets from a few IPs evicted every established peer; TLS handshake floods delayed processing (P2) | `15984089`, `9d976c19`, `0f8a9134`, `1613c84a`, `5039c292` | Accept and datagram token buckets per /24 or /64 and global; unauthenticated and established socket pools with pinning of leaf-set and routing-table peers; bounded TLS handshake executor; pinned sources bypass the per-prefix datagram budget so a spoofed flood cannot drop a neighbour's pongs |
| One peer could flood messages, hog big-message memory or trickle a message forever; Past sockets preallocated announced sizes (OOM); FileTransfer had no file cap or read timeout; maintenance requests were unbounded (P3) | `73f9c8f7`, `7750a324`, `f6ec198c`, `36922c8a`, `0407ff49` | Per-peer and per-prefix message buckets with read pausing and a per-pass cap; per-peer memory shares and deadlines; Past buffers grow with arriving bytes; FileTransfer caps and timeout; maintenance budgets |
| PAST: an insert whose `checkAndStore` failed kept its lock | `bb0ba4a3` | Lock released on every path |

## Open security findings

None from the review. Known remaining limits (by design or not worth the change so far):

- **Open relay to non-Pastry hosts (plain mode):** a plain-mode hop still relays to any routable address that passes the address policy, because a relay node cannot tell peers from strangers. Certified nodes relay nothing for others unless `pastry_socket_relay_source_routes_certified` is set.
- **Refresh authorization:** anyone can refresh an object up to the lease cap; restricting refreshes to the owner needs signed content.
- **On-path liveness spoofing:** the PING nonce only stops off-path senders; an on-path observer can still echo a ping.
- **Plain mode** trusts join answers and unauthenticated senders; use certified mode on untrusted networks.
- **Routing and joins under a colluding minority:** routing-table slots go to the lowest RTT, one bootstrap answer is trusted, routes are single-path, mutable Past lookups trust one answer, there is no revocation. Planned in `docs/security/SECURE-DESIGN.md` (phases P0 to P9) and waves C and D; privacy and metadata are wave E.
- **Past and Glacier response binding:** requests are matched by a counter and the responder is not checked (wave B2, in progress for Past, Glacier after its futures migration).

## Watch items and known limits

- **pki test hang:** not reproduced in about 660 runs. The original log showed the forked test JVM stuck at shutdown after all tests passed; the suspect is Jazzer's agent attach. If it recurs, capture a `jstack` of the fork before the worktree is removed.
- **Rendezvous/NAT and the handshake deadline:** these paths call `handshakeCompleted()` but have only unit coverage of it; watch the integration tests.
- **Budgets keyed on claimed addresses:** the priority layer's message buckets use the address a peer claims in its header, so a certified insider claiming a neighbour's address shares its bucket and pinned factor (only pauses reads). Fix in progress.
- **Simulator blind spot:** the direct simulator passes message objects, not bytes, so wire bugs show only in the socket-ring tests (`SocketRing`, `WireRoundTrip` in the pastry test-jar).

## How to work in this repo

- **Pushes go to PR #1** (`java25-upgrade` → `master` on GitHub, approved); never touch `master`. Agent work happens in a worktree branch and is cherry-picked into `java25-upgrade`.
- **Prove every fix:** the test must fail on the pre-fix code for the right reason, then pass.
- **Verify after merging:** run both full builds (default and `-Pintegration`). Parallel Maven runs share `~/.m2`, so serialize builds with a lock directory.
- **Commits** use Arlo notation (`B:`, `t:`, `D:` …) and end with the `Co-Authored-By: Claude` trailer.
- **Decisions** go to the user as a multiple-choice question. Subagents default to Sonnet; Opus only for protocol, threading or security work.
- **Wire compatibility** with old releases is not required.
- **Housekeeping:** use `trash`, not `rm`; after merging, remove agent worktrees, `git worktree prune`, delete the branches. Markdown prose is never hard-wrapped.

## Next steps

- [x] Medium and low findings of the 2026-11-05 review (2026-11-19)
- [x] Resilience wave A and wave B outside Past and Glacier (2026-11-23 to 27)
- [x] Secure design approved; P0b (SBOM, scan profile, jqwik), P2 (socket budgets), P3 (peer budgets)
- [ ] Wave B for Past and Glacier (response binding, replica checks, Glacier fragment ids and key limits)
- [ ] Secure design P0 (adversarial harness, baseline) and then P1, P4 to P9
- [ ] Wave E (privacy and metadata)
