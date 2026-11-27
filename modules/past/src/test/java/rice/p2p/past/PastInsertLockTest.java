package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;

/**
 * The insert path of PastImpl stores under the lock of the object's id; the lock must be released whatever the
 * outcome, or every later insert of that id waits for it forever (and is reported lost after the message timeout).
 */
class PastInsertLockTest {
  /** Well below the message timeout (30 s), so that an insert stuck behind a held lock fails the test. */
  private static final long SECONDS = 10;

  private SocketRing ring;
  private PastryIdFactory idFactory;

  @BeforeEach
  void setUp() {
    ring = new SocketRing();
    idFactory = new PastryIdFactory(ring.env);
  }

  @AfterEach
  void tearDown() {
    ring.close();
  }

  private PastImpl newPast(PastryNode node) {
    PastImpl past = new PastImpl(node, new CachedStorageImpl(idFactory, new MemoryStorage(idFactory), new NoOpCache(idFactory)), 1, "insert-lock");
    past.setContentDeserializer((buf, endpoint, type) -> new GCPastInsertTest.Content(buf, endpoint));
    return past;
  }

  private static Boolean[] insert(PastImpl past, PastContent content) throws Exception {
    return past.insert(content).get(SECONDS, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("an insert that the object's checkInsert refuses releases the lock, so a later insert of the id is stored")
  void refusedInsertReleasesTheLock() throws Exception {
    List<PastImpl> pasts = ring.boot(3, this::newPast);
    GCPastInsertTest.Content refused = new GCPastInsertTest.Content(idFactory.buildId("locked"), true);
    GCPastInsertTest.Content accepted = new GCPastInsertTest.Content(refused.getId(), false);

    assertThrows(ExecutionException.class, () -> insert(pasts.get(0), refused));
    Boolean[] result = insert(pasts.get(0), accepted);

    assertTrue(result[0], "the second insert was not stored");
    boolean isStored = false;
    for (PastImpl past : pasts) {
      isStored |= past.storage.exists(accepted.getId());
    }
    assertTrue(isStored, "no node stored the object");
  }
}
