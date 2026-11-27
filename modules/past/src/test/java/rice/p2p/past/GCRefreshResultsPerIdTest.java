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
import rice.Outcome;
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
    local.insert(new GCPastInsertTest.Content(existing, false), now() + 2 * HOUR_MILLIS).get(SECONDS, TimeUnit.SECONDS);
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

  private List<Outcome<Boolean>> refresh(Id[] ids, long expiration) throws Exception {
    long[] expirations = new long[ids.length];
    java.util.Arrays.fill(expirations, expiration);
    return local.refresh(ids, expirations).get(SECONDS, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("refresh of an existing id succeeds")
  void existingIdIsRefreshed() throws Exception {
    List<Outcome<Boolean>> result = refresh(new Id[] {existing}, now() + 3 * HOUR_MILLIS);

    assertEquals(1, result.size());
    assertEquals(new Outcome.Success<Boolean>(true), result.get(0));
  }

  @Test
  @DisplayName("refresh of a missing id yields ObjectNotFoundException")
  void missingIdIsNotFound() throws Exception {
    List<Outcome<Boolean>> result = refresh(new Id[] {missing}, now() + 3 * HOUR_MILLIS);

    assertInstanceOf(ObjectNotFoundException.class, assertInstanceOf(Outcome.Failure.class, result.get(0)).cause());
  }

  @Test
  @DisplayName("refresh to an earlier expiration than the current one still succeeds")
  void earlierExpirationSucceeds() throws Exception {
    List<Outcome<Boolean>> result = refresh(new Id[] {existing}, now() + HOUR_MILLIS / 2);

    assertEquals(new Outcome.Success<Boolean>(true), result.get(0));
  }

  @Test
  @DisplayName("refresh of a missing and an existing id reports each on its own")
  void mixedIdsAreReportedPerId() throws Exception {
    List<Outcome<Boolean>> result = refresh(new Id[] {missing, existing}, now() + 3 * HOUR_MILLIS);

    assertEquals(2, result.size());
    assertInstanceOf(ObjectNotFoundException.class, assertInstanceOf(Outcome.Failure.class, result.get(0)).cause());
    assertEquals(new Outcome.Success<Boolean>(true), result.get(1));
  }
}
