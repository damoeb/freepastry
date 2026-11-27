package rice.pastry.adversary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;
import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.params.Parameters;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.processing.sim.SimProcessor;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeListener;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.GenericSimulatorListener;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.selector.SelectorLoop;
import rice.selector.TimerTask;

/**
 * The adversarial harness: a direct-simulator ring of {@link Scenario#nodeCount()} nodes, a fraction of them
 * malicious with the scenario's {@link Behaviour}s, placed by its {@link Placement}, spread over failure domains,
 * optionally with {@link Churn}; it drives the workload of the scenario's timeline and measures {@link RunMetrics}
 * against the {@link Oracle}.
 *
 * <p>Deterministic: the simulator is single-threaded, every harness action runs on its selector thread as a timer task
 * in simulated time, the simulated clock starts at a fixed instant, and all draws come from seeded random sources (the
 * environment's for the protocol, a second one for the harness). Two runs of the same scenario give byte-identical
 * {@link RunMetrics#toJsonLine()} lines.
 *
 * <pre>{@code
 * RunMetrics metrics = AdversarialRing.run(Scenario.builder()
 *     .nodes(200).maliciousFraction(0.2).behaviours(new Behaviour.Drop(), new Behaviour.FastPing()).seed(1).build());
 * }</pre>
 */
public final class AdversarialRing implements Colluders, HarnessApplication.Listener {

  /** The simulated clock starts here (2001-09-09), never at the wall-clock time. */
  private static final long SIMULATED_START_MILLIS = 1_000_000_000_000L;
  private static final long CONVERGENCE_CHECK_INTERVAL_MILLIS = 2_000;
  private static final double RECOVERY_THRESHOLD = 0.99;
  /** Separates the harness random from the protocol random of the same seed. */
  private static final long HARNESS_SEED_SALT = 0x9E3779B97F4A7C15L;
  private static final long MILLIS_PER_SECOND = 1_000;
  private static final long MILLIS_PER_HOUR = 3_600_000;
  private static final long RUN_TIMEOUT_MINUTES = 60;
  private static final int RANDOM_PICK_ATTEMPTS = 64;
  private static final int OUTAGE_DOMAIN = 0;

  private final Scenario scenario;
  private final Environment environment;
  private final RandomSource random;
  private final Oracle oracle = new Oracle();
  private final FailureDomains domains;
  private final AdversarialDirectPastryNodeFactory factory;
  private final CompletableFuture<RunMetrics> result = new CompletableFuture<>();
  private final List<Lookup> lookups = new ArrayList<>();
  private final int leafSetHalf;

  private long startMillis;
  private long attackStartMillis;
  private long attackEndMillis;
  private boolean isAttacking;
  private boolean isChurning;
  private boolean isCountingMessages;
  private int maliciousCreated;
  private int notReadyAtAttackStart;
  private long honestMessages;
  private long maliciousMessages;
  private long honestBytes;
  private long droppedMessages;
  private long misroutedMessages;
  private @Nullable TableSnapshot tables;

  /** Runs the scenario to its end and returns its metrics. */
  public static RunMetrics run(Scenario scenario) throws Exception {
    AdversarialRing ring = new AdversarialRing(scenario);
    try {
      ring.environment.getSelectorManager().invoke(ring::start);
      return ring.result.get(RUN_TIMEOUT_MINUTES, TimeUnit.MINUTES);
    } finally {
      ring.environment.destroy();
    }
  }

  private AdversarialRing(Scenario scenario) {
    this.scenario = scenario;
    this.environment = simulatorEnvironment(scenario.seed());
    this.random = new SimpleRandomSource(scenario.seed() ^ HARNESS_SEED_SALT, environment.getLogManager(), "harness");
    this.leafSetHalf = environment.getParameters().getInt("pastry_lSetSize") / 2;
    this.domains = new FailureDomains(scenario.domainCount(),
        environment.getParameters().getInt("pastry_direct_max_diameter"), scenario.domainRadiusFraction(), random);
    Behaviour.FastPing fastPing = scenario.behaviour(Behaviour.FastPing.class);
    AdversarialNetwork network = new AdversarialNetwork(environment, domains, (measurer, measured) ->
        fastPing != null && !oracle.isMalicious(measurer) && oracle.isMalicious(measured) ? fastPing.factor() : 1.0);
    network.addSimulatorListener(new MessageCounter());
    this.factory = new AdversarialDirectPastryNodeFactory(new RandomNodeIdFactory(environment), network, environment,
        this);
  }

  /** A direct environment as {@link Environment#directEnvironment(int)}, with a fixed start time and seed. */
  private static Environment simulatorEnvironment(long seed) {
    Parameters parameters = new SimpleParameters(Environment.defaultParamFileArray, null);
    parameters.setString("random_seed", Long.toString(seed));
    parameters.setLong("direct_simulator_start_time", SIMULATED_START_MILLIS);
    DirectTimeSource timeSource = new DirectTimeSource(parameters);
    LogManager logManager = Environment.generateDefaultLogManager();
    timeSource.setLogManager(logManager);
    SimpleRandomSource protocolRandom = new SimpleRandomSource(seed, logManager);
    SelectorLoop selector = Environment.generateDefaultSelectorManager(timeSource, logManager, protocolRandom);
    timeSource.setSelectorManager(selector);
    return new Environment(selector, new SimProcessor(selector), protocolRandom, timeSource, logManager, parameters,
        Environment.generateDefaultExceptionStrategy(logManager));
  }

  // ----- Colluders -----

  @Override
  public boolean isAttacking() {
    return isAttacking;
  }

  @Override
  public Oracle oracle() {
    return oracle;
  }

  @Override
  public RandomSource random() {
    return random;
  }

  @Override
  public Scenario scenario() {
    return scenario;
  }

  @Override
  public void messageDropped() {
    droppedMessages++;
  }

  @Override
  public void messageMisrouted() {
    misroutedMessages++;
  }

  // ----- timeline -----

  private long now() {
    return environment.getTimeSource().currentTimeMillis();
  }

  private void at(long absoluteMillis, Runnable action) {
    environment.getSelectorManager().getTimer().schedule(new TimerTask() {
      @Override
      public void run() {
        guarded(action);
      }
    }, Math.max(0, absoluteMillis - now()));
  }

  private void every(long firstAbsoluteMillis, long periodMillis, long untilAbsoluteMillis, Runnable action) {
    environment.getSelectorManager().getTimer().schedule(new TimerTask() {
      @Override
      public void run() {
        if (now() >= untilAbsoluteMillis) {
          cancel();
          return;
        }
        guarded(action);
      }
    }, Math.max(0, firstAbsoluteMillis - now()), periodMillis);
  }

  private void guarded(Runnable action) {
    if (result.isDone()) return;
    try {
      action.run();
    } catch (RuntimeException | Error e) {
      result.completeExceptionally(e);
    }
  }

  private void start() {
    guarded(() -> {
      startMillis = now();
      int nodeCount = scenario.nodeCount();
      Set<Integer> maliciousIndices = chooseMaliciousIndices(nodeCount, Math.min(scenario.maliciousCount(), nodeCount - 1));
      for (int index = 0; index < nodeCount; index++) {
        boolean isMalicious = maliciousIndices.contains(index);
        boolean isVictim = index == 0;
        at(startMillis + index * scenario.joinIntervalMillis(), () -> createNode(isMalicious, false, isVictim));
      }
      long preAttackStartMillis = startMillis + nodeCount * scenario.joinIntervalMillis() + scenario.stabiliseMillis();
      attackStartMillis = preAttackStartMillis + scenario.preAttackWindowMillis();
      attackEndMillis = attackStartMillis + scenario.attackWindowMillis();
      long lookupPeriodMillis = Math.max(1, Math.round(MILLIS_PER_SECOND / scenario.lookupsPerSecond()));
      every(preAttackStartMillis, lookupPeriodMillis, attackEndMillis, this::startLookup);
      at(attackStartMillis, this::startAttack);
      at(attackEndMillis, this::endAttackWindow);
      at(attackEndMillis + scenario.lookupDeadlineMillis(), this::finish);
    });
  }

  /** Node 0 is the first, honest node (and the victim of an interleaved placement); the rest are drawn. */
  private Set<Integer> chooseMaliciousIndices(int nodeCount, int maliciousCount) {
    int[] candidates = new int[nodeCount - 1];
    for (int i = 0; i < candidates.length; i++) candidates[i] = i + 1;
    for (int i = candidates.length - 1; i > 0; i--) {
      int j = random.nextInt(i + 1);
      int swap = candidates[i];
      candidates[i] = candidates[j];
      candidates[j] = swap;
    }
    Set<Integer> chosen = new HashSet<>();
    for (int i = 0; i < maliciousCount; i++) chosen.add(candidates[i]);
    return chosen;
  }

  private Oracle.Member createNode(boolean isMalicious, boolean isLateJoiner, boolean isFirst) {
    Id id;
    if (isMalicious) {
      id = scenario.placement().maliciousId(random, maliciousCreated++);
    } else if (isFirst && scenario.placement() instanceof Placement.Interleaved interleaved) {
      id = interleaved.victim();
    } else {
      id = RingIds.randomId(random);
    }
    int domain = random.nextInt(domains.domainCount());
    domains.setNextDomain(domain);
    PastryNode node;
    try {
      node = factory.newNode(id, isMalicious);
    } catch (IOException e) {
      throw new IllegalStateException("could not create node " + id, e);
    }
    Oracle.Member member = new Oracle.Member(node, isMalicious, domain, isLateJoiner, now());
    oracle.created(member);
    member.application = new HarnessApplication(member, this);
    node.addPastryNodeListener(new PastryNodeListener() {
      @Override
      public void pastryNodeReady(PastryNode readyNode, boolean isReady) {
        if (isReady) oracle.ready(member, now());
      }
    });
    Oracle.Member bootstrap = randomMember(true, false);
    if (bootstrap == null) {
      node.boot((Object) null);
    } else {
      node.boot(bootstrap.node.getLocalHandle());
    }
    if (isMalicious && isAttacking) startPoisoning(member);
    return member;
  }

  /** A random live member, ready if asked, honest if asked; {@code null} if none was found. */
  private Oracle.@Nullable Member randomMember(boolean mustBeReady, boolean mustBeHonest) {
    List<Oracle.Member> live = oracle.live();
    if (live.isEmpty()) return null;
    for (int attempt = 0; attempt < RANDOM_PICK_ATTEMPTS; attempt++) {
      Oracle.Member candidate = live.get(random.nextInt(live.size()));
      if ((!mustBeReady || candidate.isReady()) && (!mustBeHonest || !candidate.isMalicious)) return candidate;
    }
    for (Oracle.Member candidate : live) {
      if ((!mustBeReady || candidate.isReady()) && (!mustBeHonest || !candidate.isMalicious)) return candidate;
    }
    return null;
  }

  private void startAttack() {
    for (Oracle.Member member : oracle.live()) {
      if (!member.isReady()) notReadyAtAttackStart++;
    }
    isAttacking = true;
    isCountingMessages = true;
    for (Oracle.Member member : List.copyOf(oracle.liveMalicious())) startPoisoning(member);
    if (scenario.hasDomainOutageAtAttackStart()) {
      for (Oracle.Member member : List.copyOf(oracle.live())) {
        if (member.domain == OUTAGE_DOMAIN) depart(member, Churn.Departure.CRASH);
      }
    }
    scenario.churn().ifPresent(this::startChurn);
    Behaviour.ChurnRotate rotate = scenario.behaviour(Behaviour.ChurnRotate.class);
    if (rotate != null) {
      for (Oracle.Member member : List.copyOf(oracle.liveMalicious())) scheduleRotation(member, rotate);
    }
    int lateJoins = scenario.lateJoinCount();
    long spacingMillis = scenario.attackWindowMillis() / 2 / (lateJoins + 1);
    for (int i = 1; i <= lateJoins; i++) {
      at(attackStartMillis + i * spacingMillis, () -> createNode(false, true, false));
    }
    every(attackStartMillis, CONVERGENCE_CHECK_INTERVAL_MILLIS, attackEndMillis, this::checkConvergence);
  }

  private void startPoisoning(Oracle.Member member) {
    Behaviour.PoisonRows rows = scenario.behaviour(Behaviour.PoisonRows.class);
    PoisoningRouteSetProtocol rowPoisoner = factory.rowPoisoner(member.id);
    if (rows != null && rowPoisoner != null) rowPoisoner.startPushing(rows.pushIntervalMillis());
    Behaviour.PoisonLeafSet leafSets = scenario.behaviour(Behaviour.PoisonLeafSet.class);
    PoisoningLeafSetProtocol leafSetPoisoner = factory.leafSetPoisoner(member.id);
    if (leafSets != null && leafSetPoisoner != null) leafSetPoisoner.startPushing(leafSets.pushIntervalMillis());
  }

  private void endAttackWindow() {
    isChurning = false;
    isCountingMessages = false;
    checkConvergence();
    tables = snapshotTables();
  }

  // ----- churn -----

  private void startChurn(Churn churn) {
    isChurning = true;
    for (Oracle.Member member : List.copyOf(oracle.live())) scheduleDeparture(member, churn);
    double arrivalsPerMillis = scenario.nodeCount() / churn.meanSessionMillis();
    scheduleArrival(churn, arrivalsPerMillis);
  }

  private void scheduleDeparture(Oracle.Member member, Churn churn) {
    at(now() + churn.drawSessionMillis(random), () -> {
      if (isChurning) depart(member, churn.departure());
    });
  }

  private void scheduleArrival(Churn churn, double arrivalsPerMillis) {
    at(now() + Churn.drawInterArrivalMillis(random, arrivalsPerMillis), () -> {
      if (!isChurning) return;
      boolean isMalicious = random.nextDouble() < scenario.maliciousFraction();
      scheduleDeparture(createNode(isMalicious, false, false), churn);
      scheduleArrival(churn, arrivalsPerMillis);
    });
  }

  private void scheduleRotation(Oracle.Member member, Behaviour.ChurnRotate rotate) {
    double meanMillis = MILLIS_PER_HOUR / rotate.rotationsPerHour();
    at(now() + Churn.drawInterArrivalMillis(random, 1 / meanMillis), () -> {
      if (!member.isLive || now() >= attackEndMillis) return;
      depart(member, Churn.Departure.CRASH);
      scheduleRotation(createNode(true, false, false), rotate);
    });
  }

  /**
   * Removes {@code member}. A crash takes it off the network first, so its own shutdown sends nothing; either way the
   * simulator tells every node at once that it is dead.
   */
  private void depart(Oracle.Member member, Churn.Departure departure) {
    if (!member.isLive) return;
    oracle.departed(member);
    if (departure == Churn.Departure.CRASH) {
      factory.network().getGenericSimulator().remove((DirectNodeHandle) member.node.getLocalHandle());
    }
    member.node.destroy();
  }

  // ----- lookups -----

  /** A lookup of the workload. */
  private static final class Lookup {
    final Id key;
    final boolean isPreAttack;
    final long sentAtMillis;
    boolean hasArrived;
    boolean isDelivered;
    long answeredAtMillis = -1;
    boolean isAnsweredCorrectly;

    Lookup(Id key, boolean isPreAttack, long sentAtMillis) {
      this.key = key;
      this.isPreAttack = isPreAttack;
      this.sentAtMillis = sentAtMillis;
    }
  }

  private void startLookup() {
    Oracle.Member origin = randomMember(true, true);
    if (origin == null || origin.application == null) return;
    long nowMillis = now();
    Lookup lookup = new Lookup(RingIds.randomId(random), nowMillis < attackStartMillis, nowMillis);
    lookups.add(lookup);
    origin.application.lookup(lookups.size() - 1, lookup.key);
  }

  @Override
  public boolean lookupArrived(long lookupId, Oracle.Member member) {
    Lookup lookup = lookups.get((int) lookupId);
    if (member.isMalicious && isAttacking && dropsAtRoot(lookup.key)) {
      droppedMessages++;
      return false;
    }
    if (!lookup.hasArrived) {
      lookup.hasArrived = true;
      lookup.isDelivered = oracle.root(lookup.key) == member;
    }
    return true;
  }

  private boolean dropsAtRoot(Id key) {
    Behaviour.Drop drop = scenario.behaviour(Behaviour.Drop.class);
    return drop != null && (drop.targetRange() == null || drop.targetRange().containsId(key))
        && random.nextDouble() < drop.probability();
  }

  @Override
  public void lookupAnswered(long lookupId, Id claimedRoot) {
    Lookup lookup = lookups.get((int) lookupId);
    if (lookup.answeredAtMillis >= 0) return;
    lookup.answeredAtMillis = now();
    Oracle.Member root = oracle.root(lookup.key);
    lookup.isAnsweredCorrectly = root != null && root.id.equals(claimedRoot)
        && lookup.answeredAtMillis - lookup.sentAtMillis <= scenario.lookupDeadlineMillis();
  }

  // ----- convergence -----

  private void checkConvergence() {
    for (Oracle.Member member : oracle.live()) {
      if (!member.isLateJoiner || member.convergedAtMillis >= 0 || !member.isReady()) continue;
      Set<Id> actual = new HashSet<>();
      for (NodeHandle handle : member.node.getLeafSet().getUniqueSet()) actual.add(handle.getNodeId());
      if (actual.equals(oracle.expectedLeafSet(member, leafSetHalf))) member.convergedAtMillis = now();
    }
  }

  // ----- messages -----

  /** Counts the messages sent in the attack window, by honest and malicious senders. */
  private final class MessageCounter implements GenericSimulatorListener<DirectNodeHandle, RawMessage> {
    @Override
    public void messageSent(RawMessage message, DirectNodeHandle from, DirectNodeHandle receiver, int delay) {
      if (!isCountingMessages) return;
      Oracle.Member sender = oracle.member(from.getNodeId());
      if (sender == null) return;
      if (sender.isMalicious) {
        maliciousMessages++;
      } else {
        honestMessages++;
        if (scenario.isByteEstimateEnabled()) honestBytes += estimateBytes(message);
      }
    }

    @Override
    public void messageReceived(RawMessage message, DirectNodeHandle from, DirectNodeHandle receiver) {}
  }

  /** The serialized size of {@code message}; 0 if it cannot be serialized outside the socket stack. */
  private static long estimateBytes(RawMessage message) {
    SimpleOutputBuffer buffer = new SimpleOutputBuffer();
    try {
      buffer.writeShort(message.getType());
      message.serialize(buffer);
      return buffer.getWritten();
    } catch (IOException | RuntimeException e) {
      return 0;
    }
  }

  // ----- metrics -----

  /** The routing-table and leaf-set metrics over the honest ready nodes. */
  private record TableSnapshot(double maliciousShareRoutingTable, double maliciousShareLeafSet,
      double routingTableDiversityMean, double routingTableDiversityP5, double leafSetDiversityMean,
      double leafSetDiversityP5) {}

  private TableSnapshot snapshotTables() {
    long routingEntries = 0;
    long maliciousRoutingEntries = 0;
    long leafSetEntries = 0;
    long maliciousLeafSetEntries = 0;
    List<Double> routingDiversity = new ArrayList<>();
    List<Double> leafSetDiversity = new ArrayList<>();
    for (Oracle.Member member : oracle.ready()) {
      if (member.isMalicious) continue;
      List<NodeHandle> routingTable = member.node.getRoutingTable().asList();
      Collection<NodeHandle> leafSet = member.node.getLeafSet().getUniqueSet();
      routingEntries += routingTable.size();
      leafSetEntries += leafSet.size();
      for (NodeHandle handle : routingTable) if (oracle.isMalicious(handle)) maliciousRoutingEntries++;
      for (NodeHandle handle : leafSet) if (oracle.isMalicious(handle)) maliciousLeafSetEntries++;
      double routing = Metrics.normalisedDomainEntropy(routingTable, oracle, domains.domainCount());
      if (routing >= 0) routingDiversity.add(routing);
      double leaves = Metrics.normalisedDomainEntropy(leafSet, oracle, domains.domainCount());
      if (leaves >= 0) leafSetDiversity.add(leaves);
    }
    return new TableSnapshot(Metrics.ratio(maliciousRoutingEntries, routingEntries),
        Metrics.ratio(maliciousLeafSetEntries, leafSetEntries), Metrics.mean(routingDiversity),
        Metrics.percentile5(routingDiversity), Metrics.mean(leafSetDiversity), Metrics.percentile5(leafSetDiversity));
  }

  private void finish() {
    TableSnapshot snapshot = tables;
    if (snapshot == null) throw new IllegalStateException("the attack window did not end");
    long preSent = 0;
    long preDelivered = 0;
    long preAnswered = 0;
    long sent = 0;
    long delivered = 0;
    long answered = 0;
    for (Lookup lookup : lookups) {
      if (lookup.isPreAttack) {
        preSent++;
        if (lookup.isDelivered) preDelivered++;
        if (lookup.isAnsweredCorrectly) preAnswered++;
      } else {
        sent++;
        if (lookup.isDelivered) delivered++;
        if (lookup.isAnsweredCorrectly) answered++;
      }
    }
    double preDeliveryRatio = Metrics.ratio(preDelivered, preSent);

    long lateJoinersCounted = 0;
    List<Double> convergenceMillis = new ArrayList<>();
    for (Oracle.Member member : allLateJoiners()) {
      if (member.convergedAtMillis >= 0) {
        lateJoinersCounted++;
        convergenceMillis.add((double) (member.convergedAtMillis - member.createdAtMillis));
      } else if (member.isLive) {
        lateJoinersCounted++;
      }
    }
    double convergenceMax = convergenceMillis.stream().mapToDouble(Double::doubleValue).max().orElse(-1);

    result.complete(new RunMetrics(scenario, preDeliveryRatio, Metrics.ratio(preAnswered, preSent),
        Metrics.ratio(delivered, sent), Metrics.ratio(answered, sent), sent, snapshot.maliciousShareRoutingTable(),
        snapshot.maliciousShareLeafSet(), snapshot.routingTableDiversityMean(), snapshot.routingTableDiversityP5(),
        snapshot.leafSetDiversityMean(), snapshot.leafSetDiversityP5(),
        Metrics.ratio(convergenceMillis.size(), lateJoinersCounted), Metrics.mean(convergenceMillis), convergenceMax,
        recoveryMillis(preDeliveryRatio), Metrics.ratio(honestMessages, sent),
        scenario.isByteEstimateEnabled() ? Metrics.ratio(honestBytes, sent) : -1,
        Metrics.ratio(honestMessages, maliciousMessages), droppedMessages, misroutedMessages, notReadyAtAttackStart));
  }

  private List<Oracle.Member> allLateJoiners() {
    List<Oracle.Member> lateJoiners = new ArrayList<>();
    for (Oracle.Member member : oracle.everCreated()) {
      if (member.isLateJoiner) lateJoiners.add(member);
    }
    return lateJoiners;
  }

  /** See {@link RunMetrics#recoveryMillis()}. */
  private long recoveryMillis(double preDeliveryRatio) {
    long bucketMillis = scenario.recoveryBucketMillis();
    int bucketCount = (int) ((scenario.attackWindowMillis() + bucketMillis - 1) / bucketMillis);
    long[] sentPerBucket = new long[bucketCount];
    long[] deliveredPerBucket = new long[bucketCount];
    for (Lookup lookup : lookups) {
      if (lookup.isPreAttack) continue;
      int bucket = (int) Math.min(bucketCount - 1, (lookup.sentAtMillis - attackStartMillis) / bucketMillis);
      sentPerBucket[bucket]++;
      if (lookup.isDelivered) deliveredPerBucket[bucket]++;
    }
    double threshold = RECOVERY_THRESHOLD * preDeliveryRatio;
    int lastLowBucket = -1;
    int lastBucketWithLookups = -1;
    for (int bucket = 0; bucket < bucketCount; bucket++) {
      if (sentPerBucket[bucket] == 0) continue;
      lastBucketWithLookups = bucket;
      if ((double) deliveredPerBucket[bucket] / sentPerBucket[bucket] < threshold) lastLowBucket = bucket;
    }
    if (lastLowBucket < 0) return 0;
    if (lastLowBucket == lastBucketWithLookups) return -1;
    return (lastLowBucket + 1) * bucketMillis;
  }
}
