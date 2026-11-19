package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.FutureContinuation;
import rice.p2p.commonapi.Id;
import rice.p2p.past.gc.ExpiringPastImpl;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;

/**
 * The default-build counterpart of the integration test GCPastRefreshTest (direct simulator): a refresh reports success per
 * existing id and ObjectNotFoundException per missing id, on a ring of socket nodes on loopback.
 */
class GCRefreshResultsPerIdTest {
  private static final long SECONDS = 10;
  private static final long HOUR_MILLIS = 3_600_000L;
  private static final int REPLICAS = 3;

  private SocketRing ring;
  private PastryIdFactory idFactory;
  private ExpiringPastImpl local;
  private Id existing;
  private Id missing;

  @BeforeEach
  void setUp() throws Exception {
    ring = new SocketRing();
    idFactory = new PastryIdFactory(ring.env);
    List<ExpiringPastImpl> pasts = ring.boot(REPLICAS, this::newPast);
    local = pasts.get(0);
    existing = idFactory.buildId("existing");
    missing = idFactory.buildId("missing");
    FutureContinuation.<Boolean[]>call(c -> local.insert(new GCPastInsertTest.Content(existing, false), now() + 2 * HOUR_MILLIS, c)).get(SECONDS, TimeUnit.SECONDS);
  }

  @AfterEach
  void tearDown() {
    ring.close();
  }

  private ExpiringPastImpl newPast(PastryNode node) {
    ExpiringPastImpl past = new ExpiringPastImpl(node, new CachedStorageImpl(idFactory, new MemoryStorage(idFactory), new NoOpCache(idFactory)), null,
        REPLICAS, "gc-refresh-results", new PastPolicy.DefaultPastPolicy(), 100_000_000L, null);
    past.setContentDeserializer((buf, endpoint, type) -> new GCPastInsertTest.Content(buf, endpoint));
    past.setContentHandleDeserializer((buf, endpoint, type) -> {
      throw new IOException("no handles in this test");
    });
    return past;
  }

  private long now() {
    return ring.env.getTimeSource().currentTimeMillis();
  }

  private Object[] refresh(Id[] ids, long expiration) throws Exception {
    long[] expirations = new long[ids.length];
    java.util.Arrays.fill(expirations, expiration);
    return FutureContinuation.<Object[]>call(c -> local.refresh(ids, expirations, c)).get(SECONDS, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("refresh of an existing id succeeds")
  void existingIdIsRefreshed() throws Exception {
    Object[] result = refresh(new Id[] {existing}, now() + 3 * HOUR_MILLIS);

    assertEquals(1, result.length);
    assertEquals(true, result[0]);
  }

  @Test
  @DisplayName("refresh of a missing id yields ObjectNotFoundException")
  void missingIdIsNotFound() throws Exception {
    Object[] result = refresh(new Id[] {missing}, now() + 3 * HOUR_MILLIS);

    assertInstanceOf(ObjectNotFoundException.class, result[0]);
  }

  @Test
  @DisplayName("refresh to an earlier expiration than the current one still succeeds")
  void earlierExpirationSucceeds() throws Exception {
    Object[] result = refresh(new Id[] {existing}, now() + HOUR_MILLIS / 2);

    assertEquals(true, result[0]);
  }

  @Test
  @DisplayName("refresh of a missing and an existing id reports each on its own")
  void mixedIdsAreReportedPerId() throws Exception {
    Object[] result = refresh(new Id[] {missing, existing}, now() + 3 * HOUR_MILLIS);

    assertEquals(2, result.length);
    assertInstanceOf(ObjectNotFoundException.class, result[0]);
    assertEquals(true, result[1]);
  }
}
