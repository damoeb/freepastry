package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.FutureContinuation;
import rice.p2p.commonapi.Id;
import rice.p2p.past.testing.DistPastTestContent;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.CachedStorageImpl;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;

/**
 * A peer that answers with content it made up. Content-hash objects are immutable and self-certifying, so a node
 * must not store, return or cache an object whose payload does not hash to its id, and a replica fetch must store
 * exactly the id it asked for.
 */
class PastForgedContentTest {
  private static final long SECONDS = 10;
  private static final int CACHE_BYTES = 512 * 1024;
  private static final int REPLICAS = 1;

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

  private static <R> R await(java.util.concurrent.CompletableFuture<R> future) throws Exception {
    return future.get(SECONDS, TimeUnit.SECONDS);
  }

  private PastImpl newPast(PastryNode node, PastPolicy policy) {
    PastImpl past = new PastImpl(node, new CachedStorageImpl(idFactory, new MemoryStorage(idFactory),
        new LRUCache(new MemoryStorage(idFactory), CACHE_BYTES, node.getEnvironment())), REPLICAS, "forged", policy);
    past.setContentDeserializer((buf, endpoint, type) -> {
      if (type == DistPastTestContent.TYPE) {
        return new DistPastTestContent(buf, endpoint);
      }
      throw new IOException("Unknown content type " + type);
    });
    return past;
  }

  private List<PastImpl> boot(int size) throws Exception {
    return ring.boot(size, node -> newPast(node, new PastPolicy.DefaultPastPolicy()));
  }

  /** Content that claims the hash of {@code claimedPayload} but carries {@code realPayload}. */
  private DistPastTestContent forged(String claimedPayload, String realPayload) {
    return new DistPastTestContent(idFactory.buildId(claimedPayload), realPayload, 0);
  }

  private void put(PastImpl past, DistPastTestContent content) throws Exception {
    await(past.storage.getStorage().store(content.getId(), null, content));
  }

  private PastContent stored(PastImpl past, Id id) throws Exception {
    return (PastContent) await(past.storage.getObject(id));
  }

  @Test
  @DisplayName("an insert of content whose payload does not hash to its id is refused and nothing is stored")
  void insertOfForgedContentIsRefused() throws Exception {
    List<PastImpl> pasts = boot(3);
    DistPastTestContent evil = forged("victim", "evil");

    assertThrows(ExecutionException.class,
        () -> await(FutureContinuation.<Boolean[]>call(continuation -> pasts.get(0).insert(evil, continuation))));

    for (PastImpl past : pasts) {
      assertNull(stored(past, evil.getId()), "forged content stored on a replica");
    }
  }

  @Test
  @DisplayName("a lookup does not return forged content a replica holds, and does not cache it")
  void lookupDoesNotReturnForgedContent() throws Exception {
    List<PastImpl> pasts = boot(3);
    DistPastTestContent evil = forged("victim", "evil");
    for (PastImpl past : pasts.subList(1, pasts.size())) {
      put(past, evil);
    }

    PastContent found = null;
    try {
      found = await(FutureContinuation.<PastContent>call(continuation -> pasts.get(0).lookup(evil.getId(), true, continuation)));
    } catch (ExecutionException expected) {
      // refusing with an exception is as good as returning nothing
    }

    assertNull(found, "lookup returned forged content");
    assertFalse(pasts.get(0).storage.exists(evil.getId()), "forged content was cached by the lookup");
  }

  @Test
  @DisplayName("a content-hash object whose payload does not hash to its id is not cached")
  void cacheRefusesForgedContent() throws Exception {
    List<PastImpl> pasts = boot(1);
    DistPastTestContent evil = forged("victim", "evil");

    await(FutureContinuation.<Boolean>call(continuation -> pasts.get(0).cache(evil, continuation)));

    assertNull(stored(pasts.get(0), evil.getId()), "forged content was cached");
  }
}
