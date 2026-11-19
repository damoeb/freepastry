# FreePastry

A modular, open-source implementation of the [Pastry](http://freepastry.org/) peer-to-peer structured overlay network.

## Requirements

- Java 25 (LTS) or higher
- Apache Maven 3.9+ (or use the bundled wrapper: `./mvnw`, pinned to Maven 3.10.0)

With [SDKMAN!](https://sdkman.io/), run `sdk env install` in the project root to get the exact JDK and Maven versions from `.sdkmanrc`.

The compiler runs with `-Xlint:rawtypes,deprecation,removal,dep-ann -Werror`: raw (untyped) generic types and calls to deprecated APIs fail the build. Failing tests fail the build too.

`./mvnw verify` also runs Spotless: unused or wildcard imports, trailing whitespace, tabs and a missing final newline fail the build. Run `./mvnw spotless:apply` to fix them. Spotless does not reflow code.

## Quick Start

Use the bundled Maven wrapper; it downloads Maven 3.10.0 on first use. Dependencies come from Maven Central only, as configured in `.mvn/settings.xml`.

FreePastry is a multi-module Maven build; see [Modules](#modules). Run every command below from the project root. Add `-pl modules/<name> -am` to build one module together with the modules it depends on.

### Build

```bash
./mvnw clean compile
```

This compiles each module into its `modules/<name>/target/classes/` directory.

### Create JARs

```bash
./mvnw package
```

This runs the tests and creates one jar per module, `modules/<name>/target/freepastry-<name>-2.1.0.jar`, plus its test classes as `freepastry-<name>-2.1.0-tests.jar`. It also builds the jar with dependencies (see below).

### Run Tests

```bash
./mvnw verify
```

`verify` compiles, runs the unit tests (JUnit Jupiter) and checks the formatting with Spotless. Long simulations and real-network tests are tagged `integration` and are excluded by default; run them with `./mvnw verify -Pintegration`.

To run a specific test (modules without a matching test are skipped):

```bash
./mvnw test -Dtest=LivenessTest
```

To run the tests of one module only, for example PAST:

```bash
./mvnw test -pl modules/past -am
```

Tests run sequentially to avoid port conflicts. A failing test fails the build.

### Create JAR with Dependencies

```bash
./mvnw package -DskipTests
```

This creates `modules/all/target/freepastry-2.1.0-jar-with-dependencies.jar`, which bundles every FreePastry module with Bouncy Castle and the SLF4J API. The distribution module `freepastry-all` builds it during `package`, so no `assembly:single` is needed. It contains no SLF4J backend: add one to the classpath (see [Logging](#logging)) to see log output.

### Run Hello World Example

The example programs are in each module's `src/test/java` and the tutorials in `modules/tutorials/src/main/java/rice/tutorial/`. Run a tutorial with the jar with dependencies, for example a node that starts a new ring on port 9001 (it boots from its own address):

```bash
java -cp modules/all/target/freepastry-2.1.0-jar-with-dependencies.jar rice.tutorial.lesson3.DistTutorial 9001 <your-ip> 9001
```

Start additional nodes that join the ring through the first one:

```bash
java -cp modules/all/target/freepastry-2.1.0-jar-with-dependencies.jar rice.tutorial.lesson3.DistTutorial 9002 <first-node-ip> 9001
```

## Common Maven Commands

- `./mvnw clean` - Remove generated files
- `./mvnw compile` - Compile source files
- `./mvnw test` - Run tests
- `./mvnw verify` - Run tests and the Spotless check
- `./mvnw spotless:apply` - Fix formatting violations
- `./mvnw package` - Create the module JARs and the JAR with dependencies
- `./mvnw package -DskipTests -pl modules/all -am` - Create only the JAR with dependencies
- `./mvnw compile javadoc:javadoc` - Generate API documentation per module (output in `modules/<name>/target/reports/apidocs/`)
- `./mvnw javadoc:aggregate` - Generate one API documentation for all modules (output in `target/reports/apidocs/`)
- `./mvnw install` - Install all modules to the local Maven repository

## Modules

Every module has the group id `org.mpisws.p2p`, the version `2.1.0` and the artifact id `freepastry-<name>`. Each module depends only on the modules listed for it (and on what those depend on).

| Module | Directory | Packages | Depends on |
|---|---|---|---|
| `freepastry-core` | `modules/core` | `rice` (Continuation, FutureContinuation, Destructable, Executable), `rice.environment`, `rice.selector`, `rice.p2p.commonapi`, `rice.p2p.util`, and `freepastry.params` | Bouncy Castle (bcprov), SLF4J API |
| `freepastry-transport` | `modules/transport` | `org.mpisws.p2p.transport`, except `peerreview` | core |
| `freepastry-filetransfer` | `modules/filetransfer` | `org.mpisws.p2p.filetransfer` | core, transport |
| `freepastry-pastry` | `modules/pastry` | `rice.pastry` | core, transport |
| `freepastry-pki` | `modules/pki` | `org.mpisws.p2p.pki` | core, transport, pastry, Bouncy Castle (bcpkix) |
| `freepastry-peerreview` | `modules/peerreview` | `org.mpisws.p2p.transport.peerreview` | core, transport, pki |
| `freepastry-persistence` | `modules/persistence` | `rice.persistence` | core |
| `freepastry-past` | `modules/past` | `rice.p2p.past`, `rice.p2p.replication` | core, persistence |
| `freepastry-scribe` | `modules/scribe` | `rice.p2p.scribe`, `rice.p2p.splitstream` | core, pastry |
| `freepastry-multiring` | `modules/multiring` | `rice.p2p.multiring` | core, pastry, scribe |
| `freepastry-glacier` | `modules/glacier` | `rice.p2p.glacier`, `rice.p2p.aggregation` | core, pastry, past, multiring, persistence |
| `freepastry-tutorials` | `modules/tutorials` | `rice.tutorial` | all of the above |
| `freepastry-all` | `modules/all` | none: builds the jar with dependencies | all of the above |

Tests live in the module whose code they test. A test that needs a higher module lives in the lowest module that sees everything it uses, for example `SSLTest` in pki and the Id-set tests in glacier. `MessageTypeRegistriesTest`, which checks every message type registry, is in tutorials because that module depends on all others. Some modules use other modules in test scope only: persistence, past and filetransfer test with pastry ids, and peerreview's replay programs use scribe. Shared test helpers (`TLTest` and `FreePorts` in transport, `CommonAPITest` and `RegressionRun` in pastry, the unit programs in core) ship in their module's test-jar.

## Project Structure

- `pom.xml` - Parent POM: the module list, dependency and plugin versions, and the build configuration all modules share
- `modules/<name>/pom.xml` - One module's dependencies
- `modules/<name>/src/main/java/` - Java source files
- `modules/core/src/main/resources/` - Default configuration (`freepastry.params`)
- `modules/<name>/src/test/java/` - Tests (JUnit Jupiter) and regression programs (`testing` packages)
- `modules/<name>/target/` - Compiled classes and JARs (generated by Maven)
- `docs/` - Architecture overview, protocol notes and diagrams
- `html/rawtutorial/` - Tutorial files

## Dependencies

FreePastry uses the following libraries (managed by Maven):

- Bouncy Castle (bcprov, bcpkix) - Cryptography and X.509 certificates (PKI tooling, SSL transport)
- SLF4J API 2 (`slf4j-api`) - Logging facade; see [Logging](#logging)
- JSpecify (`org.jspecify:jspecify`) - Nullness annotations (`@NullMarked`, `@Nullable`); see [Static analysis](#static-analysis)
- JUnit 6 (Jupiter) - Testing framework
- slf4j-simple (test scope only) - Logging backend for the tests

## Static analysis

Every module compiles with [Error Prone](https://errorprone.info/) and, for the `@NullMarked` packages, [NullAway](https://github.com/uber/NullAway). Both run inside javac as part of `./mvnw compile`, and with `-Werror` every finding fails the build. The versions and flags are in the parent `pom.xml` (`errorprone.*` and `nullaway.version` properties); the `jdk.compiler` exports Error Prone needs are in `.mvn/jvm.config`.

- Error Prone runs on main and test code. All its checks, warnings included, are on in every module, and `-Werror` makes any finding fail the build. The checks that are off for the whole build are listed with their reasons in the parent `pom.xml`.
- NullAway checks only code in `@NullMarked` packages, in JSpecify mode. For now these are all packages of `freepastry-core` and `freepastry-transport` (see their `package-info.java`); test code is not checked. In those packages every type is non-null unless it is annotated `@Nullable` (`org.jspecify.annotations.Nullable`, a type-use annotation: `String @Nullable []` for a nullable array, `Map.@Nullable Entry` for a nested type). Where a value is set after construction and always before use, the field carries `@SuppressWarnings("NullAway.Init")`; where null is impossible once the object is set up, the code says so with `Objects.requireNonNull` and a message.
- To suppress a finding that is intended, put `@SuppressWarnings("CheckName")` (for example `"ReferenceEquality"` or `"NullAway"`) on the narrowest declaration that covers it, a method, field or local variable, and say why in a comment on the same line. To turn a check off everywhere, add `-Xep:CheckName:OFF` to `errorprone.flags` in the parent `pom.xml`, with the reason in the comment above it.

## Fuzzing

The raw-message deserializers read bytes from untrusted peers, so they are fuzzed with [Jazzer](https://github.com/CodeIntelligenceTesting/jazzer) (`jazzer-junit`, test scope, version in the parent `pom.xml`). The invariant of every target: given arbitrary bytes, deserialization returns a value or throws `IOException`. Any other exception (`RuntimeException`, `OutOfMemoryError`, `NegativeArraySizeException`, `ClassCastException`, `IndexOutOfBoundsException`, `StackOverflowError`) or a huge allocation is a finding.

The fuzz tests are `@FuzzTest` methods in classes named `*FuzzTest`: `AddressFuzzTest`, `PriorityFuzzTest` and `NetworkInfoFuzzTest` (transport), `PastryWireFuzzTest` (pastry), `PastFuzzTest`, `ScribeFuzzTest`, `MultiringFuzzTest`, `GlacierFuzzTest` (glacier and aggregation), `PeerReviewFuzzTest` and `PkiFuzzTest`. Shared helpers are in `rice.testing.fuzz.FuzzSupport` (core test classes) and `org.mpisws.p2p.transport.fuzz.FuzzSocket` (transport test classes).

In a normal `./mvnw verify` Jazzer runs in regression mode: each target only replays the seed inputs committed under `src/test/resources/<package>/<TestClass>Inputs/<method>/`, which is fast and deterministic. Every crash that fuzzing found is kept there as a regression seed.

To fuzz, set `JAZZER_FUZZ=1` and run one target of one module (Jazzer fuzzes only one `@FuzzTest` per run; each runs for the `maxDuration` of its annotation, 30 seconds):

```bash
JAZZER_FUZZ=1 ./mvnw test -pl modules/pastry -am -Dtest='PastryWireFuzzTest#tlDeserializer' -Dsurefire.failIfNoSpecifiedTests=false
```

The coverage-guided corpus of a run is kept in `.cifuzz-corpus/` of the module (ignored by git). A crashing input is saved as a `crash-<hash>` file in the seed directory of the target and replays on the next regression run; fix the bug, then keep the file as a regression seed. To keep a good corpus, copy the files of `.cifuzz-corpus/<TestClass>/<method>/` into the seed directory.

## Logging

FreePastry logs through the [SLF4J](https://www.slf4j.org/) 2 API and does not bring a logging backend of its own. Put exactly one SLF4J backend on your application's classpath, for example `org.slf4j:slf4j-simple` for console output or `ch.qos.logback:logback-classic` for files and rotation. Without a backend, SLF4J prints one warning and discards all log output.

Levels, line format and output files are configured in the backend, for example in `simplelogger.properties` (`org.slf4j.simpleLogger.defaultLogLevel=info`, `org.slf4j.simpleLogger.log.rice.pastry.socket=debug`) or in `logback.xml`. Each logger is named after its class, so package prefixes work as usual. A logger created with an instance name gets `.<instance>` appended, and each node of a multi-node JVM appends `.<node id>` (for example `rice.pastry.PastryNode.0x1A2B3C`), so you can also set the level of a single node.

The old `freepastry.params` logging parameters (`loglevel`, `<logger>_loglevel`, `logging_*`, `log_rotate_*`, `environment_logToFile`, `fileLogManager_*`) are gone: set levels and log files in the backend instead. The tests use slf4j-simple with each module's `src/test/resources/simplelogger.properties`, which logs at `warn` and above.

## Documentation

- Architecture overview for new developers (threading, transport stack, apps): [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Javadoc API documentation: run `./mvnw javadoc:aggregate`; the output is in `target/reports/apidocs/` (open `index.html`). `./mvnw compile javadoc:javadoc` writes one per module to `modules/<name>/target/reports/apidocs/`. It is generated, not committed.
- Tutorial: `html/rawtutorial/index.html`, with the example sources in `modules/tutorials/src/main/java/rice/tutorial/`
- Configuration: the defaults and a comment for every parameter are in `modules/core/src/main/resources/freepastry.params`. Components read them into typed config records (`*Config.fromParameters(Parameters)`) when they are constructed.

## Serialization

Everything on the wire and on disk is raw-serialized; there is no Java serialization. A message implements `RawMessage` (`getType()` and `serialize(OutputBuffer)`), and each application registers a `MessageDeserializer` with `endpoint.setDeserializer()`. PAST and Scribe content implements `PastContent` and `ScribeContent` in the same way. `PersistentStorage` writes objects through a `StorageSerializer`. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#serialization).

## Cryptography

`rice.p2p.util.SecurityUtils` and `HashUtils` provide the primitives: AES-256-GCM for symmetric encryption, SHA-256 for hashes and ids, HMAC-SHA256, RSA-OAEP with SHA-256 for asymmetric encryption, and SHA256withRSA signatures. Password-encrypted key pair files (the CA and ring key pairs) derive their key with PBKDF2-HMAC-SHA256. These use the JDK providers; Bouncy Castle is used explicitly (`SecurityUtils.BOUNCY_CASTLE`) for X.509 certificate generation and is not registered as a JVM-wide provider.

### Trusted networks only, by default

The default socket transport does no authentication or encryption. Use it only on trusted networks: anyone who can reach a node's port can join the ring, claim any node id (spoof a node), read the traffic, or surround a node with colluding identities (eclipse it).

For untrusted networks, insert `org.mpisws.p2p.transport.ssl.SSLTransportLayer` (mutual TLS for the TCP sockets) and bind node ids to certificates. A certificate authority (CA) issues each node one X.509 certificate whose common name is the node id (`CN=<40 hex digits>`); a node can only use the id in its certificate, and peers refuse a connection whose claimed id differs from the certificate. An id a node merely hears about (a handle in routing state, a datagram) is unverified until a TLS connection presents a certificate for it. The design is in the Javadoc of `rice.pastry.standard.NodeIdCertificate`.

1. Create a CA and one PKCS12 keystore per node with `org.mpisws.p2p.pki.x509.CATool`: `-ca <name> -p <ca password> -store` creates the CA on first use and writes `<node id>.store` (blank password) with the node's key, its certificate and the CA certificate. Without `-cn` the node gets a random node id; with `-cn` it must be a node id in hex. In code, `CATool.signNodeId(id, publicKey)` issues the certificate.
2. Take the node id from the certificate with `CertifiedNodeIdFactory.fromKeyStore()` or `load()`. It checks that the CA signed the certificate, that the certificate and the CA certificate are valid now, and that the node holds the certificate's private key, and throws a `SecurityException` otherwise.
3. Create the node with `rice.pastry.socket.CertifiedSocketPastryNodeFactory(keyStore, keyPassword, port, env)` instead of `SocketPastryNodeFactory`; it does steps 2 and 3 for you and nothing needs subclassing. It takes the node id from the certificate, wraps the transport in an `SSLTransportLayer` with client authentication required and the node's keystore as key and trust store (TLS validates the peer's chain against the CA and the validity period), and is strict by default: a handshake with a peer whose certificate common name is not a node id fails (`NodeIdCertificate::isNodeId`). Its `CertifiedIdBindStrategy` tracks each id as unverified, authenticated or refused: a handle learned over the wire carries no certificate, so its id is unverified; a TLS connection whose certificate common name equals the id (accepted or opened by this node) promotes it to authenticated; one whose common name differs is refused, and a warning is logged. `rice.tutorial.ssl.DistTutorial` is the working example. To assemble the layers yourself, pass `NodeIdCertificate::isNodeId` as the subject validator of `SSLTransportLayer` (without a validator any common name passes).

What an unauthenticated id may and may not do in certified mode:

- Routing table and leaf set: a handle is admitted only once its id is authenticated. When a handle is offered whose id is not, the node opens a TLS connection to it (and closes it again at once); if the certificate matches, the handle is admitted then, otherwise it stays out. Until then the node does not treat it as a member of the ring (its neighbour, a replica holder, a route hop).
- Application messages (anything an `Endpoint` routes, including `route(null, msg, hint)` to one node): a message that arrives from another node reaches the application (`forward`, then `deliver` or the next hop) only if the id of its previous hop, the node it arrived from, is authenticated. Messages from a hop not yet verified trigger the check and wait for it in a small bounded queue (8 per peer, 64 in all, up to 10 seconds, selector thread only): they go on in order once the id is verified, and are dropped if the certificate does not match, the connection is refused, the check times out, or a bound is exceeded. The transport is unreliable anyway, so applications already retry. Messages created on the node itself are not gated.
- Only the previous hop is authenticated, not the originator: the sender named in a routed message (and anything the payload says about where it comes from) is whatever the originating node, or any authenticated node on the way, put there. An application that needs to know who sent a message must sign it and check the signature.
- Outgoing connections check the peer's certificate against the id the node connected to, just as incoming ones do.
- Datagrams: UDP is outside TLS, so a datagram proves nothing about its sender. A received message goes up to Pastry (join, leaf set and route set maintenance, routed messages) and to the applications only if it came over a TLS connection whose certificate is the sender's id; a datagram never does, whatever sender its header names, and is dropped. A message that asks to be sent as a datagram (`WireTransportLayer.OPTION_TRANSPORT_TYPE` = `TRANSPORT_TYPE_DATAGRAM`) goes over TLS instead. Only the transport's own traffic below the identity layer still travels over UDP: liveness pings and pongs (which only say that an address answers) and network-info probes.
- Bindings: the transport binds each address to one node handle, and a handle with the same address and a newer epoch replaces the old one and marks it dead for good (a restarted node). Only a certificate may do that to a node whose id is authenticated: a handle offered without one (a datagram's header, a handle in a message) never replaces it, so a forged datagram cannot evict an authenticated neighbour. A handle that comes with its certificate replaces an unverified one whatever the epochs. A restarted node is therefore recognised once it connects over TLS, which it does when it rejoins. The bindings are bounded (`pastry_socket_identity_max_bindings`, least recently used forgotten first), but a binding confirmed by a certificate is never forgotten to make room.
- Not gated, on purpose: routing towards an id (a node must route to ids it has not met, and a joining node must reach the ring before anyone has authenticated it) and connecting to a handle; the originator named in a routed (multi-hop) message, because it is not our TLS peer and only a signature by that node could prove it (see above); the node's own protocols (join, leaf set and route set maintenance), which have the admission test of the leaf set and routing table instead.

Limits: authentication is by node id only and is not bound to the address the peer connects from: the certificate proves the id, the address is not authenticated, and a node may change its address and keep its id. An unverified id is only upgraded by a direct TLS connection with that peer, so a colluding neighbour can advertise ids it does not own, but they stay out of the routing table and leaf set; the CA signs node certificates directly (no intermediate CAs, no revocation list; a certificate stops working when it expires, so issue short validity periods), and the trust anchor is whatever CA certificate is in the node's keystore.

TLS covers sockets only: liveness pings and pongs and network-info probes, which still go over UDP, are unauthenticated and unencrypted (in certified mode nothing above them uses UDP, see above). See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#cryptography).

## Applications

The applications are libraries, not standalone programs. Each one runs inside a Pastry node: you create a node with a `PastryNodeFactory`, construct the application on it (for example `new ScribeImpl(node, "myInstance")`), and boot the node. Every node in the ring that takes part runs its own instance of the application. The tutorials below show this end to end and are the quickest way to start each application.

| Application | Module | Purpose | Entry point |
|---|---|---|---|
| **Pastry** | `freepastry-pastry` | The overlay itself: a self-organizing ring of nodes with 160-bit ids. It routes a message to the live node whose id is numerically closest to a key in O(log N) hops, and repairs its routing tables as nodes join and fail. All other applications build on it. | `SocketPastryNodeFactory`, `DirectPastryNodeFactory` (simulator) |
| **PAST** | `freepastry-past` | Distributed hash table: `insert` stores an object on the k nodes closest to its id, `lookup` fetches it from the nearest replica. Content is immutable; a cache along the route speeds up repeated lookups. | `PastImpl(node, storageManager, replicas, instance)` |
| **GCPast** | `freepastry-past` | PAST with expiration times: objects are refreshed by their owner and garbage-collected after they expire. | `GCPastImpl` |
| **Replication** | `freepastry-past` | Keeps a set of keys replicated on the k closest nodes as the leaf set changes, by periodically comparing Bloom filters with neighbours. PAST uses it; you can use it for your own storage. | `ReplicationImpl(node, client, replicationFactor, instance)` |
| **Scribe** | `freepastry-scribe` | Publish/subscribe and multicast: nodes subscribe to a topic, Scribe builds a multicast tree rooted at the node closest to the topic id, and `publish` delivers a message to all subscribers. It also supports anycast (deliver to one subscriber). | `ScribeImpl(node, instance)` |
| **SplitStream** | `freepastry-scribe` | High-bandwidth streaming on top of Scribe: a channel is split into 16 stripes, each with its own tree, so the forwarding load is spread over all nodes instead of a few interior nodes. | `SplitStreamImpl(node, instance)` |
| **Multiring** | `freepastry-multiring` | Lets one node be part of several Pastry rings (for example an organization ring and a global ring) and route between them with ring-qualified ids. | `MultiringNode(ringId, node)` |
| **Glacier** | `freepastry-glacier` | Durable archival storage: objects are erasure-coded into fragments spread over many nodes, so they survive large correlated failures. | `GlacierImpl` |
| **Aggregation** | `freepastry-glacier` | Bundles many small objects into larger aggregates before storing them in Glacier or PAST, which keeps the per-object overhead low. | `AggregationImpl` |
| **PeerReview** | `freepastry-peerreview` | Accountability: each node keeps a tamper-evident, signed log of its messages; peers audit each other's logs and expose nodes that deviate from the protocol. | `PeerReviewImpl` |
| **File transfer** | `freepastry-filetransfer` | Sends files and large byte buffers between two nodes over an application socket, with priorities and progress callbacks. | `FileTransferImpl` |

### Start the applications

Build the jar with dependencies first (see [Create JAR with Dependencies](#create-jar-with-dependencies)). The jar contains no SLF4J backend, so the tutorials only show their own console output. To also see FreePastry's log, add a backend to the classpath, for example the slf4j-simple jar that the build has already downloaded:

```bash
JAR=modules/all/target/freepastry-2.1.0-jar-with-dependencies.jar
CP=$JAR:$HOME/.m2/repository/org/slf4j/slf4j-simple/2.0.20/slf4j-simple-2.0.20.jar
```

Networked tutorials take `localbindport bootIP bootPort [numNodes]`. The first node of a new ring boots from its own address; further nodes, in the same or another JVM or on another machine, boot from any running node. `numNodes` starts that many nodes in one JVM, on consecutive ports. For example, start a ring of 10 PAST nodes and add 5 more:

```bash
java -cp $CP rice.tutorial.past.PastTutorial 9001 <your-ip> 9001 10
java -cp $CP rice.tutorial.past.PastTutorial 9100 <your-ip> 9001 5
```

Simulator tutorials (`-direct` or `DirectTutorial`) run all nodes in one JVM over a simulated network and need no ports.

| Tutorial | Shows | Command (after `java -cp $CP`) |
|---|---|---|
| `rice.tutorial.lesson1.DistTutorial` | Starting one node and joining a ring | `rice.tutorial.lesson1.DistTutorial 9001 <boot-ip> 9001` |
| `rice.tutorial.lesson3.DistTutorial` | A minimal application that routes messages by key and directly to a node | `rice.tutorial.lesson3.DistTutorial 9001 <boot-ip> 9001` |
| `rice.tutorial.lesson4.DistTutorial` | Several nodes in one JVM | `rice.tutorial.lesson4.DistTutorial 9001 <boot-ip> 9001 10` |
| `rice.tutorial.direct.DirectTutorial` | The same application on the network simulator | `rice.tutorial.direct.DirectTutorial 20` |
| `rice.tutorial.deterministicsimulator.DirectTutorial` | A reproducible simulation from a seed | `rice.tutorial.deterministicsimulator.DirectTutorial 20 42` |
| `rice.tutorial.past.PastTutorial` | PAST: insert objects, then look them up from other nodes | `rice.tutorial.past.PastTutorial 9001 <boot-ip> 9001 10` |
| `rice.tutorial.scribe.ScribeTutorial` | Scribe: subscribe to a topic, publish and anycast | `rice.tutorial.scribe.ScribeTutorial 9001 <boot-ip> 9001 10` |
| `rice.tutorial.splitstream.SplitStreamTutorial` | SplitStream: a channel with stripes | `rice.tutorial.splitstream.SplitStreamTutorial 9001 <boot-ip> 9001 10`, or `-direct 20` |
| `rice.tutorial.lookup.LookupServiceTest` | Find the node responsible for an id | `rice.tutorial.lookup.LookupServiceTest 9001 <boot-ip> 9001 10` |
| `rice.tutorial.sendfile.Tutorial` | File transfer between two nodes | `rice.tutorial.sendfile.Tutorial 9001 <boot-ip> 9001 2`, or `-direct 2` |
| `rice.tutorial.appsocket.Tutorial` | Stream sockets between applications | `rice.tutorial.appsocket.Tutorial 9001 <boot-ip> 9001 2`, or `-direct 2` |
| `rice.tutorial.timer.DistTutorial` | Scheduling periodic tasks on the selector thread | `rice.tutorial.timer.DistTutorial 9001 <boot-ip> 9001` |
| `rice.tutorial.ssl.DistTutorial` | Nodes that authenticate each other with TLS | `rice.tutorial.ssl.DistTutorial 9001 <boot-ip> 9001 <keystore.p12>` |
| `rice.tutorial.transportlayer.DistTutorial` | Adding your own transport layer (here a bandwidth limiter) | `rice.tutorial.transportlayer.DistTutorial 9001 <boot-ip> 9001 10 1000` |

The step-by-step explanations for these programs are in `html/rawtutorial/index.html`, and their sources are in `modules/tutorials/src/main/java/rice/tutorial/`. Glacier, Aggregation, Multiring and PeerReview have no tutorial; their tests (`modules/<module>/src/test/java`) show how to set them up.

## License

See `license/License.txt` for licensing information.

