package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.FutureContinuation;
import rice.p2p.commonapi.Id;
import rice.p2p.past.gc.ExpiringId;
import rice.p2p.past.gc.ExpiringPastImpl;
import rice.p2p.past.gc.ExpiringPastMetadata;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.CachedStorage;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;

/**
 * An insert, a refresh and a replication fetch can lease an object only up to p2p_past_gc_max_lease, and a refresh cannot
 * bring an object back out of the trash: the refresh carries no proof that its sender owns the object.
 */
class GCRefreshLeaseTest {
  private static final long SECONDS = 10;
  private static final long HOUR = 3_600_000L;
  private static final long DAY = 24 * HOUR;
  /** Generous upper bound of the default p2p_past_gc_max_lease (one year), plus a day of slack. */
  private static final long MAX_LEASE_BOUND = 366 * DAY;

  private SocketRing ring;
  private PastryIdFactory idf;
  private final List<CachedStorage> trashes = new ArrayList<>();

  @BeforeEach
  void setUp() {
    ring = new SocketRing();
    idf = new PastryIdFactory(ring.env);
  }

  @AfterEach
  void tearDown() {
    ring.close();
  }

  private ExpiringPastImpl newPast(PastryNode node) {
    CachedStorage trash = new CachedStorageImpl(idf, new MemoryStorage(idf), new NoOpCache(idf));
    trashes.add(trash);
    ExpiringPastImpl past = new ExpiringPastImpl(node, new CachedStorageImpl(idf, new MemoryStorage(idf), new NoOpCache(idf)), null, 1,
        "gc-lease", new PastPolicy.DefaultPastPolicy(), 100_000_000L, trash);
    past.setContentDeserializer((buf, endpoint, type) -> new GCPastInsertTest.Content(buf, endpoint));
    past.setContentHandleDeserializer((buf, endpoint, type) -> {
      throw new IOException("no handles in this test");
    });
    return past;
  }

  /** Waits until the object is stored on that many nodes: the replication runs asynchronously to the insert. */
  private void awaitHolders(List<ExpiringPastImpl> pasts, Id id, int expected) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SECONDS);
    while (pasts.stream().filter(past -> past.storage.exists(id)).count() < expected) {
      assertTrue(System.nanoTime() < deadline, "the object was not replicated to " + expected + " nodes");
      Thread.sleep(10);
    }
  }

  private long now() {
    return ring.env.getTimeSource().currentTimeMillis();
  }

  @Test
  @DisplayName("a refresh to Long.MAX_VALUE leases the object only up to the maximum lease")
  void refreshExpirationIsCapped() throws Exception {
    List<ExpiringPastImpl> pasts = ring.boot(3, this::newPast);
    Id id = idf.buildId("lease");
    long start = now();
    FutureContinuation.<Boolean[]>call(c -> pasts.get(0).insert(new GCPastInsertTest.Content(id, false), start + HOUR, c)).get(SECONDS, TimeUnit.SECONDS);

    Object[] result = FutureContinuation.<Object[]>call(c -> pasts.get(0).refresh(new Id[] {id}, new long[] {Long.MAX_VALUE}, c)).get(SECONDS, TimeUnit.SECONDS);

    assertEquals(true, result[0]);
    int holders = 0;
    for (ExpiringPastImpl past : pasts) {
      if (past.storage.exists(id)) {
        holders++;
        long expiration = ((ExpiringPastMetadata) past.storage.getMetadata(id)).expiration();
        assertTrue(expiration > start + HOUR, "the refresh did not extend the lease: " + expiration);
        assertTrue(expiration <= now() + MAX_LEASE_BOUND, "lease not capped: " + expiration);
      }
    }
    assertTrue(holders > 0, "no node holds the object");
  }

  @Test
  @DisplayName("an insert with Long.MAX_VALUE leases the object only up to the maximum lease")
  void insertExpirationIsCapped() throws Exception {
    List<ExpiringPastImpl> pasts = ring.boot(3, this::newPast);
    Id id = idf.buildId("insert-lease");

    FutureContinuation.<Boolean[]>call(c -> pasts.get(0).insert(new GCPastInsertTest.Content(id, false), Long.MAX_VALUE, c)).get(SECONDS, TimeUnit.SECONDS);

    awaitHolders(pasts, id, 1);
    for (ExpiringPastImpl past : pasts) {
      if (past.storage.exists(id)) {
        long expiration = ((ExpiringPastMetadata) past.storage.getMetadata(id)).expiration();
        assertTrue(expiration <= now() + MAX_LEASE_BOUND, "lease not capped: " + expiration);
      }
    }
  }

  @Test
  @DisplayName("a replica fetched from a peer is leased only up to the maximum lease, whatever expiration the peer names")
  void fetchedReplicaExpirationIsCapped() throws Exception {
    List<ExpiringPastImpl> pasts = ring.boot(3, this::newPast);
    Id id = idf.buildId("fetch-lease");
    FutureContinuation.<Boolean[]>call(c -> pasts.get(0).insert(new GCPastInsertTest.Content(id, false), now() + HOUR, c)).get(SECONDS, TimeUnit.SECONDS);
    awaitHolders(pasts, id, 1);
    ExpiringPastImpl holder = pasts.stream().filter(past -> past.storage.exists(id)).findFirst().orElseThrow();
    ExpiringPastImpl fetcher = pasts.stream().filter(past -> !past.storage.exists(id)).findFirst().orElseThrow();

    // a replica that is not stored yet, and one that is stored and whose lease the peer wants to raise
    assertEquals(true, FutureContinuation.<Boolean>call(c -> fetcher.fetch(new ExpiringId(id, Long.MAX_VALUE), holder.getLocalNodeHandle(), c)).get(SECONDS, TimeUnit.SECONDS));
    assertEquals(true, FutureContinuation.<Boolean>call(c -> holder.fetch(new ExpiringId(id, Long.MAX_VALUE), fetcher.getLocalNodeHandle(), c)).get(SECONDS, TimeUnit.SECONDS));

    for (ExpiringPastImpl past : List.of(holder, fetcher)) {
      long expiration = ((ExpiringPastMetadata) past.storage.getMetadata(id)).expiration();
      assertTrue(expiration <= now() + MAX_LEASE_BOUND, "lease not capped: " + expiration);
    }
  }

  @Test
  @DisplayName("a refresh does not bring an object back out of the trash")
  void refreshDoesNotResurrect() throws Exception {
    List<ExpiringPastImpl> pasts = ring.boot(3, this::newPast);
    Id id = idf.buildId("trashed");
    GCPastInsertTest.Content content = new GCPastInsertTest.Content(id, false);
    FutureContinuation.<Boolean[]>call(c -> pasts.get(0).insert(content, now() + HOUR, c)).get(SECONDS, TimeUnit.SECONDS);
    // the insert returns once the root stores the object: the replication to the other replicas is still under way and
    // would store the object behind the trash's back
    awaitHolders(pasts, id, 2);
    // what the garbage collection does with an expired object: into the trash, out of the storage
    for (int i = 0; i < pasts.size(); i++) {
      ExpiringPastImpl past = pasts.get(i);
      CachedStorage trash = trashes.get(i);
      if (past.storage.exists(id)) {
        trash.store(id, new ExpiringPastMetadata(1), content).get(SECONDS, TimeUnit.SECONDS);
        past.storage.unstore(id).get(SECONDS, TimeUnit.SECONDS);
      }
    }

    Object[] result = FutureContinuation.<Object[]>call(c -> pasts.get(0).refresh(new Id[] {id}, new long[] {now() + HOUR}, c)).get(SECONDS, TimeUnit.SECONDS);

    assertInstanceOf(ObjectNotFoundException.class, result[0]);
    for (ExpiringPastImpl past : pasts) {
      assertFalse(past.storage.exists(id), "refresh restored a trashed object");
    }
  }
}
