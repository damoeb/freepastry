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
import rice.p2p.past.gc.ExpiringPastImpl;
import rice.p2p.past.gc.ExpiringPastMetadata;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.EmptyCache;
import rice.persistence.MemoryStorage;
import rice.persistence.StorageManager;
import rice.persistence.StorageManagerImpl;

/**
 * A refresh can neither lease an object beyond p2p_past_gc_max_lease nor bring an object back out of the trash: the
 * refresh carries no proof that its sender owns the object.
 */
class GCRefreshLeaseTest {
  private static final long SECONDS = 10;
  private static final long HOUR = 3_600_000L;
  private static final long DAY = 24 * HOUR;
  /** Generous upper bound of the default p2p_past_gc_max_lease (one year), plus a day of slack. */
  private static final long MAX_LEASE_BOUND = 366 * DAY;

  private SocketRing ring;
  private PastryIdFactory idf;
  private final List<StorageManager> trashes = new ArrayList<>();

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
    StorageManager trash = new StorageManagerImpl(idf, new MemoryStorage(idf), new EmptyCache(idf));
    trashes.add(trash);
    ExpiringPastImpl past = new ExpiringPastImpl(node, new StorageManagerImpl(idf, new MemoryStorage(idf), new EmptyCache(idf)), null, 1,
        "gc-lease", new PastPolicy.DefaultPastPolicy(), 100_000_000L, trash);
    past.setContentDeserializer((buf, endpoint, type) -> new GCPastInsertTest.Content(buf, endpoint));
    past.setContentHandleDeserializer((buf, endpoint, type) -> {
      throw new IOException("no handles in this test");
    });
    return past;
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
  @DisplayName("a refresh does not bring an object back out of the trash")
  void refreshDoesNotResurrect() throws Exception {
    List<ExpiringPastImpl> pasts = ring.boot(3, this::newPast);
    Id id = idf.buildId("trashed");
    GCPastInsertTest.Content content = new GCPastInsertTest.Content(id, false);
    FutureContinuation.<Boolean[]>call(c -> pasts.get(0).insert(content, now() + HOUR, c)).get(SECONDS, TimeUnit.SECONDS);
    // what the garbage collection does with an expired object: into the trash, out of the storage
    for (int i = 0; i < pasts.size(); i++) {
      ExpiringPastImpl past = pasts.get(i);
      StorageManager trash = trashes.get(i);
      if (past.storage.exists(id)) {
        FutureContinuation.<Boolean>call(c -> trash.store(id, new ExpiringPastMetadata(1), content, c)).get(SECONDS, TimeUnit.SECONDS);
        FutureContinuation.<Boolean>call(c -> past.storage.unstore(id, c)).get(SECONDS, TimeUnit.SECONDS);
      }
    }

    Object[] result = FutureContinuation.<Object[]>call(c -> pasts.get(0).refresh(new Id[] {id}, new long[] {now() + HOUR}, c)).get(SECONDS, TimeUnit.SECONDS);

    assertInstanceOf(ObjectNotFoundException.class, result[0]);
    for (ExpiringPastImpl past : pasts) {
      assertFalse(past.storage.exists(id), "refresh restored a trashed object");
    }
  }
}
