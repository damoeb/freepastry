# FreePastry security handoff

2026-11-19

Every finding of the 2026-11-05 security review is fixed on `java25-upgrade` (tip `04c03ea1`, not pushed): critical and high on 2026-11-05, medium and low on 2026-11-19. What remains is a short list of known limits and watch items.

## Where things stand

| Item | State |
| --- | --- |
| Branch | `java25-upgrade` (the user calls it "develop"), tip `04c03ea1`, not pushed, tree clean |
| Default build | `./mvnw -B clean verify`: BUILD SUCCESS, 1250 tests, 0 failures, 0 skipped |
| Integration build | `./mvnw -B clean verify -Pintegration`: BUILD SUCCESS, 11 tests, about 20 min. It runs only the integration-tagged tests, so run both builds |
| Java | `export JAVA_HOME=$HOME/.sdkman/candidates/java/25.0.5.0+9-amzn PATH=$JAVA_HOME/bin:$PATH` before `./mvnw` |
| Other branches | `master`, `upgdate`, `wip-ep-snapshot` (old, user-made). No agent worktrees or branches left |
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
| Handshake slowloris and unbounded UDP drain (W9) | `44225fd1` | Incoming sockets must finish the handshake within `transport_wire_handshake_timeout_millis` (10 s); at most `transport_wire_max_handshaking_sockets_per_address` (32) per IP; at most `transport_wire_max_datagrams_per_read` (64) datagrams per selection. API: `P2PSocket.handshakeCompleted()`; code using the wire layer without a priority layer must call it |
| A forged PONG with a guessed timestamp marked a route alive | `5ee5348a` | PING and PONG carry a random 64-bit nonce; a PONG counts only if it echoes an outstanding ping's nonce. API change: `LivenessTransportLayer.pong(..., senderNonce, ...)` |
| Node certificates lacked extensions; the node's own leaf was a trust anchor; TLS version from JDK defaults | `42ce5682` | Node certs: basicConstraints CA:false, keyUsage, extendedKeyUsage serverAuth+clientAuth, random 128-bit serials. Trust store holds only the CA (`CertifiedSocketPastryNodeFactory.trustStoreOf`). TLS 1.3 only |
| `IdentityTransportLayerImpl.verify` threw RuntimeException on a malformed signature; predictable PeerReview temp file | `75c5ff1e` | A malformed signature is `SIGNATURE_BAD`; temp histories live in a private 0700 directory |
| Maven wrapper without checksum | `f8ffb0b7` | `distributionSha256Sum` pinned |
| `user.params` in the working directory merged over defaults; `nat_handler_class` initialized before its type check | `04c03ea1` | A params file is read only when the caller names it; the class is type-checked before initialization |

Behaviour change in certified mode: a node restarting at the same address is recognised only once it reconnects over TLS, which it does when it rejoins.

## Open security findings

None from the review. Known remaining limits (by design or not worth the change so far):

- **Open relay to non-Pastry hosts:** a hop still relays to any other routable address that passes the address policy, because a relay node cannot tell peers from strangers.
- **Refresh authorization:** anyone can refresh an object up to the lease cap; restricting refreshes to the owner needs signed content.
- **On-path liveness spoofing:** the PING nonce only stops off-path senders; an on-path observer can still echo a ping.
- **Plain mode** trusts join answers and unauthenticated senders; use certified mode on untrusted networks.
- **CA certificate** is still a v1 certificate without a CA:true extension.

## Watch items and known limits

- **pki test hang:** not reproduced in about 660 runs. The original log showed the forked test JVM stuck at shutdown after all tests passed; the suspect is Jazzer's agent attach. If it recurs, capture a `jstack` of the fork before the worktree is removed.
- **Direct re-send priority:** a non-routed message whose deserializer ignores the wire priority goes out at its default priority if an application re-sends the received object. A central fix needs `setPriority` on `commonapi.Message` plus a test per module.
- **Rendezvous/NAT and the handshake deadline:** these paths call `handshakeCompleted()` but have only unit coverage of it; watch the integration tests.
- **Log volume:** the "per-address handshake cap reached" message is logged at info, one line per refused connection under a flood.
- **Simulator blind spot:** the direct simulator passes message objects, not bytes, so wire bugs show only in the socket-ring tests (`SocketRing`, `WireRoundTrip` in the pastry test-jar).

## How to work in this repo

- **No PRs, no push** unless the user asks; never touch `master`. Agent work happens in a worktree branch and is cherry-picked into `java25-upgrade`.
- **Prove every fix:** the test must fail on the pre-fix code for the right reason, then pass.
- **Verify after merging:** run both full builds (default and `-Pintegration`). Parallel Maven runs share `~/.m2`, so serialize builds with a lock directory.
- **Commits** use Arlo notation (`B:`, `t:`, `D:` …) and end with the `Co-Authored-By: Claude` trailer.
- **Decisions** go to the user as a multiple-choice question. Subagents default to Sonnet; Opus only for protocol, threading or security work.
- **Wire compatibility** with old releases is not required.
- **Housekeeping:** use `trash`, not `rm`; after merging, remove agent worktrees, `git worktree prune`, delete the branches. Markdown prose is never hard-wrapped.

## Next steps

- [x] Fix the medium findings (2026-11-19, all eight)
- [x] Close the CA and key-store password and file-permission issues
- [x] Replace or gate the Glacier default signature policy before anyone relies on Glacier integrity
- [x] Fix the low findings (2026-11-19)
- [ ] Decide on the direct re-send priority fix and the remaining limits above
- [ ] Push `java25-upgrade` and open a PR with `bb` when the user asks
