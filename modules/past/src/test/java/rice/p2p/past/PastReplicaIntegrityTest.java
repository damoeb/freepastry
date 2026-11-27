package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.past.gc.ExpiringId;
import rice.p2p.past.gc.ExpiringPastImpl;
import rice.p2p.past.gc.ExpiringPastMetadata;
import rice.p2p.past.gc.GCIdRange;
import rice.p2p.past.gc.GCIdFactory;
import rice.p2p.past.messaging.InsertMessage;
import rice.p2p.replication.ReplicationImpl;
import rice.p2p.replication.messaging.ReplicationResponseMessage;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.Cache;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;

/**
 * The replicas of an id keep what they store to themselves: a node stores an insert only if it is a replica of the
 * id, acts only on the replication responses it asked for, and a replica it fetches passes the object's checkInsert.
 *
 * <p>The ring is 00, 80, A0 with one extra replica: the replicas of id 30 are 00 and 80, and A0 is not one.
 */
class PastReplicaIntegrityTest {
  private static final long SECONDS = 10;
  private static final long EXPIRATION = 1_000_000_000_000_000L;
  private static final long QUIET_MILLIS = 3000;

  private SocketRing ring;
  private PastryIdFactory idFactory;
  private final Id id = rice.pastry.Id.build("30");

  @BeforeEach
  void setUp() {
    ring = new SocketRing();
    // no maintenance round of its own during a test: a test starts one when it needs one
    ring.env.getParameters().setInt("p2p_replication_maintenance_interval", Integer.MAX_VALUE);
    idFactory = new PastryIdFactory(ring.env);
  }

  @AfterEach
  void tearDown() {
    ring.close();
  }

  private <T> T onSelector(Supplier<CompletableFuture<T>> operation) throws Exception {
    CompletableFuture<CompletableFuture<T>> started = new CompletableFuture<CompletableFuture<T>>();
    ring.env.getSelectorManager().invoke(() -> started.complete(operation.get()));
    return started.get(SECONDS, TimeUnit.SECONDS).get(SECONDS, TimeUnit.SECONDS);
  }

  private void runOnSelector(Runnable task) throws Exception {
    onSelector(() -> {
      task.run();
      return CompletableFuture.completedFuture(null);
    });
  }

  private PastImpl newPast(PastryNode node) {
    PastImpl past = new PastImpl(node, new CachedStorageImpl(idFactory, new MemoryStorage(idFactory), new NoOpCache(idFactory)), 1, "integrity");
    past.setContentDeserializer((buf, endpoint, type) -> new GCPastInsertTest.Content(buf, endpoint));
    return past;
  }

  /** A policy that records the replicas it is asked to fetch and answers with what the function gives. */
  private static final class RecordingPolicy implements PastPolicy {
    final List<Id> fetched = new CopyOnWriteArrayList<Id>();
    private final Function<Id, Object> answer;

    RecordingPolicy(Function<Id, Object> answer) {
      this.answer = answer;
    }

    @Override
    public CompletableFuture<Object> fetch(Id fetchedId, NodeHandle hint, Cache backup, Past past) {
      fetched.add(fetchedId);
      return CompletableFuture.completedFuture(answer.apply(fetchedId));
    }

    @Override
    public boolean isInsertAllowed(PastContent content) {
      return true;
    }
  }

  private ExpiringPastImpl newExpiringPast(PastryNode node, PastPolicy policy) {
    ExpiringPastImpl past = new ExpiringPastImpl(node, new CachedStorageImpl(idFactory, new MemoryStorage(idFactory), new NoOpCache(idFactory)), null, 1,
        "integrity", policy, 100_000_000L, null);
    past.setContentDeserializer((buf, endpoint, type) -> new GCPastInsertTest.Content(buf, endpoint));
    return past;
  }

  @Test
  @DisplayName("a node that is not a replica of the id refuses an insert sent to it directly, and stores nothing")
  void insertOnNonReplicaIsRefused() throws Exception {
    List<PastImpl> pasts = ring.boot(3, this::newPast);
    PastImpl insider = pasts.get(0);
    PastImpl nonReplica = pasts.get(2);
    GCPastInsertTest.Content content = new GCPastInsertTest.Content(id, false);

    Boolean isStored = onSelector(() -> insider.<Boolean>sendRequest(nonReplica.getLocalNodeHandle(),
        new InsertMessage(insider.nextRequestId(), content, insider.getLocalNodeHandle(), nonReplica.getLocalNodeHandle().getId())));

    assertFalse(isStored, "the non-replica acknowledged the insert");
    assertFalse(nonReplica.storage.exists(id), "the non-replica stored the insert");
  }

  @Test
  @DisplayName("a replica still stores an insert")
  void insertOnReplicaIsStored() throws Exception {
    List<PastImpl> pasts = ring.boot(3, this::newPast);
    GCPastInsertTest.Content content = new GCPastInsertTest.Content(id, false);

    Boolean[] stored = onSelector(() -> pasts.get(2).insert(content));

    assertEquals(List.of(true, true), List.of(stored));
    assertTrue(pasts.get(0).storage.exists(id) && pasts.get(1).storage.exists(id), "a replica did not store the insert");
    assertFalse(pasts.get(2).storage.exists(id), "the non-replica stored the insert");
  }

  @Test
  @DisplayName("a replication response the node did not ask for starts no fetch")
  void unsolicitedReplicationResponseIsDropped() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(fetchedId -> null);
    List<ExpiringPastImpl> pasts = ring.boot(3, node -> newExpiringPast(node, policy));
    ExpiringPastImpl victim = pasts.get(0);
    NodeHandle insider = pasts.get(1).getLocalNodeHandle();
    IdSet offered = new GCIdFactory(idFactory).buildIdSet();
    offered.addId(new ExpiringId(id, EXPIRATION));
    IdRange range = new GCIdRange(new rice.pastry.IdRange(rice.pastry.Id.build("20"), rice.pastry.Id.build("40")));

    runOnSelector(() -> ((ReplicationImpl) victim.getReplication())
        .deliver(null, new ReplicationResponseMessage(insider, new IdRange[] {range}, new IdSet[] {offered}), insider));
    Thread.sleep(QUIET_MILLIS);

    assertEquals(List.of(), policy.fetched, "the unsolicited response made the node fetch");
  }

  @Test
  @DisplayName("a replication response to a request of the node still makes it fetch what it lacks")
  void solicitedReplicationResponseIsFetched() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(fetchedId -> null);
    List<ExpiringPastImpl> pasts = ring.boot(3, node -> newExpiringPast(node, policy));
    ExpiringPastImpl holder = pasts.get(1);
    onSelector(() -> holder.storage.getStorage().store(id, new ExpiringPastMetadata(EXPIRATION), new GCPastInsertTest.Content(id, false)));

    runOnSelector(() -> pasts.get(0).getReplication().replicate());

    ring.await("the node fetched the object it lacks", SECONDS, () -> policy.fetched.contains(id));
  }

  @Test
  @DisplayName("a replica that ExpiringPast fetches is not stored if the object's checkInsert refuses it")
  void expiringReplicaFetchRunsCheckInsert() throws Exception {
    RecordingPolicy policy = new RecordingPolicy(fetchedId -> new GCPastInsertTest.Content(fetchedId, true));
    ExpiringPastImpl past = ring.boot(1, node -> newExpiringPast(node, policy)).get(0);

    Boolean isFetched = onSelector(() -> past.fetch(new ExpiringId(id, EXPIRATION), null));

    assertFalse(isFetched, "a refused replica counts as fetched");
    assertFalse(past.storage.exists(id), "a replica that checkInsert refuses was stored");
  }
}
