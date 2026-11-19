# FreePastry security handoff

2026-11-10

All critical and high findings from the 2026-11-05 security review are fixed on `java25-upgrade` (tip `46e20438`, not pushed); what remains is the medium and low findings, one certified-mode admission gap, and a few watch items.

## Where things stand

| Item | State |
| --- | --- |
| Branch | `java25-upgrade` (the user calls it "develop"), tip `46e20438`, not pushed, tree clean |
| Default build | `./mvnw -B clean verify`: BUILD SUCCESS, 1096 tests, 0 failures, 0 skipped |
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
| Unbounded tables: spoofed identities and routes grew `IdentityLayerCore.bindings` and the liveness trackers until OOM; a forged pong marked a route alive | `71d10ec4` | Both tables are LRU-bounded (new params `pastry_socket_identity_max_bindings`, `pastry_socket_liveness_max_trackers`, default 10000); bindings with pending messages and, in certified mode, bindings confirmed by a certificate (`BindStrategy.keepsBinding`, new) are never forgotten; the dead-forever set is bounded too. A pong counts only if it echoes an outstanding ping of that route. API change: `LivenessConfig` gains `maxTrackers` |
| Big-message preallocation (`BigMessageReader`) | `ba70378d` | The buffer grows with the bytes received (8 KB start, doubling); a per-node budget `pastry_socket_writer_max_big_msg_in_flight` (default 64 MB) closes sockets that would exceed it. API change: `PriorityConfig` has a fourth component `maxBigMsgInFlight` |
| CA tool and key stores: empty CA password, `-p` in `ps`, blank node key password, 0644 files | `b1e8465d` | `CertificateAuthorityTool` refuses an empty password and reads it from `FREEPASTRY_CA_PASSWORD` or the console; `-p` is refused. `-store` node stores get a password from `FREEPASTRY_NODE_PASSWORD` or the console. `SSLTransportLayer` has a constructor taking the key password, which `CertifiedSocketPastryNodeFactory` passes. Key store files are written 0600 (`CertificateAuthorityTool.newPrivateOutputStream`). API change: `CertificateAuthorityTool -p` removed; `DistTutorial` needs the node store password |
| GC refresh accepted any expiration and restored trashed objects | `c4deb99c` | A refresh is capped to now + `p2p_past_gc_max_lease` (default 365 days) and never restores from the trash. Remaining: authorizing a refresh needs signed content; an insert can still ask for any expiration, and replication fetch trusts a peer's expiration |
| Glacier default "signature" was the key's own bytes: forged manifests, fragment squatting, endless lifetime | `c3cb6528` | Owner-signed manifests: the storage id derives from the owner's public key and the object's name, the manifest carries both and a SHA256withRSA signature; every receiving path verifies; only the owner can extend. API change: `GlacierDefaultPolicy` needs a `KeyPair` and the `IdFactory`, `GlacierPolicy.storageId()` is new, Glacier ids are per owner; the stub is `InsecureGlacierPolicy` in the test sources |

Behaviour change in certified mode: a node restarting at the same address is recognised only once it reconnects over TLS, which it does when it rejoins.

## Open security findings

None of these has been reproduced yet; each needs a failing test before its fix. Severity is the review's.

| Finding | Severity | Risk | Suggested fix |
| --- | --- | --- | --- |
| Admission checks the id only (`CertifiedSocketPastryNodeFactory.isAuthenticatedOrVerify`) | Medium | An authenticated insider can advertise another trusted node's id at its own address; the handle is admitted without a probe | Admit per (id, address, epoch), probing the handle's address |
| Join-window trust (`ConsistentJoinProtocol`) | Medium | A node that is still joining trusts any accepted JoinRequest | Needs authentication of the accepting node, not a bounds check |
| Handshake slowloris | Low | No deadline after the magic number; half-open sockets evict real peers via LimitSockets | Handshake deadline up to the priority layer; per-IP cap |
| Smaller items | Low | Probe reflection via forged ids; node certificates lack basicConstraints/keyUsage; `IdentityTransportLayerImpl.verify` throws RuntimeException; predictable PeerReview temp file; Maven wrapper without checksum; `user.params` in CWD merged over defaults | See the review file, section Low |

## Watch items and known limits

- **pki test hang:** not reproduced in about 660 runs. The original log showed the forked test JVM stuck at shutdown after all tests passed; the suspect is Jazzer's agent attach. If it recurs, capture a `jstack` of the fork before the worktree is removed.
- **Aggregate counts are shorts:** more than 32,767 pointers or components per aggregate breaks.
- **Source routes are IPv4-only** on the wire (`WireSourceRoute`).
- **Direct re-send priority:** a message re-sent directly, not routed, goes out at its default priority.
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

- [ ] Decide whether to fix the medium findings, and in which order (the certified-mode admission gap first if untrusted networks matter)
- [ ] Close the CA and key-store password and file-permission issues (small, local, low risk)
- [x] Replace or gate the Glacier default signature policy before anyone relies on Glacier integrity
- [ ] Push `java25-upgrade` and open a PR with `bb` when the user asks
