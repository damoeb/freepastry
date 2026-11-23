package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.FutureContinuation;
import rice.p2p.past.testing.DistPastTestContent;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.CachedStorageImpl;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;

/**
 * The paths of a lookup: the local store first, then the node the key routes to, then the replicas. Replies are
 * cached only when the caller asked for it.
 */
class PastLookupPathsTest {
  private static final long SECONDS = 10;
  private static final int CACHE_BYTES = 512 * 1024;

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

  private PastImpl newPast(PastryNode node) {
    PastImpl past = new PastImpl(node, new CachedStorageImpl(idFactory, new MemoryStorage(idFactory),
        new LRUCache(new MemoryStorage(idFactory), CACHE_BYTES, node.getEnvironment())), 1, "lookup");
    past.setContentDeserializer((buf, endpoint, type) -> {
      if (type == DistPastTestContent.TYPE) {
        return new DistPastTestContent(buf, endpoint);
      }
      throw new IOException("Unknown content type " + type);
    });
    return past;
  }

  private void put(PastImpl past, DistPastTestContent content) throws Exception {
    await(past.storage.getStorage().store(content.getId(), null, content));
  }

  private PastContent lookup(PastImpl past, DistPastTestContent content, boolean cache) throws Exception {
    return await(FutureContinuation.<PastContent>call(continuation -> past.lookup(content.getId(), cache, continuation)));
  }

  @Test
  @DisplayName("a lookup of a locally stored object answers from the local store")
  void lookupFindsLocalObject() throws Exception {
    List<PastImpl> pasts = ring.boot(3, this::newPast);
    DistPastTestContent content = new DistPastTestContent(ring.env, idFactory, "local");
    put(pasts.get(0), content);

    PastContent found = lookup(pasts.get(0), content, false);

    assertEquals("local", ((DistPastTestContent) found).getContent());
  }

  @Test
  @DisplayName("a lookup of a remote object returns it and caches it when asked to")
  void lookupOfRemoteObjectCachesWhenAsked() throws Exception {
    List<PastImpl> pasts = ring.boot(3, this::newPast);
    DistPastTestContent content = new DistPastTestContent(ring.env, idFactory, "remote");
    put(pasts.get(1), content);
    put(pasts.get(2), content);

    PastContent found = lookup(pasts.get(0), content, true);

    assertEquals("remote", ((DistPastTestContent) found).getContent());
    assertTrue(pasts.get(0).storage.getCache().exists(content.getId()), "reply was not cached");
  }

  @Test
  @DisplayName("a lookup of a remote object does not cache it when not asked to")
  void lookupOfRemoteObjectDoesNotCacheWhenNotAsked() throws Exception {
    List<PastImpl> pasts = ring.boot(3, this::newPast);
    DistPastTestContent content = new DistPastTestContent(ring.env, idFactory, "remote");
    put(pasts.get(1), content);
    put(pasts.get(2), content);

    PastContent found = lookup(pasts.get(0), content, false);

    assertEquals("remote", ((DistPastTestContent) found).getContent());
    assertFalse(pasts.get(0).storage.getCache().exists(content.getId()), "reply was cached");
  }

  @Test
  @DisplayName("a lookup of an object no node holds answers null")
  void lookupOfMissingObjectAnswersNull() throws Exception {
    List<PastImpl> pasts = ring.boot(3, this::newPast);

    assertNull(lookup(pasts.get(0), new DistPastTestContent(ring.env, idFactory, "missing"), true));
  }
}
