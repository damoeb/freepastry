package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.Continuation;
import rice.FutureContinuation;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.past.testing.DistPastTestContent;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.Cache;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;
import rice.persistence.StorageManagerImpl;

/**
 * A replica peer answers a replication fetch with content under another id. The fetching node must store only what
 * it asked for, or the peer can overwrite any object, immutable ones included.
 */
class PastReplicationFetchTest {
  private static final long SECONDS = 10;
  private static final int CACHE_BYTES = 512 * 1024;

  private SocketRing ring;
  private PastryIdFactory idf;

  @BeforeEach
  void setUp() {
    ring = new SocketRing();
    idf = new PastryIdFactory(ring.env);
  }

  @AfterEach
  void tearDown() {
    ring.close();
  }

  private static <R> R await(java.util.concurrent.CompletableFuture<R> future) throws Exception {
    return future.get(SECONDS, TimeUnit.SECONDS);
  }

  private PastImpl newPast(PastryNode node, PastPolicy policy) {
    PastImpl past = new PastImpl(node, new StorageManagerImpl(idf, new MemoryStorage(idf),
        new LRUCache(new MemoryStorage(idf), CACHE_BYTES, node.getEnvironment())), 1, "fetch", policy);
    past.setContentDeserializer((buf, endpoint, type) -> {
      if (type == DistPastTestContent.TYPE) {
        return new DistPastTestContent(buf, endpoint);
      }
      throw new IOException("Unknown content type " + type);
    });
    return past;
  }

  private PastImpl bootReplying(Object reply) throws Exception {
    PastPolicy lying = new PastPolicy() {
      @Override
      public void fetch(Id id, NodeHandle hint, Cache backup, Past past, Continuation<? super Object, Exception> command) {
        command.receiveResult(reply);
      }

      @Override
      public boolean allowInsert(PastContent content) {
        return true;
      }
    };
    return ring.<PastImpl>boot(1, node -> newPast(node, lying)).get(0);
  }

  private void put(PastImpl past, DistPastTestContent content) throws Exception {
    await(FutureContinuation.<Boolean>call(c -> past.storage.getStorage().store(content.getId(), null, content, c)));
  }

  private PastContent stored(PastImpl past, Id id) throws Exception {
    return (PastContent) await(FutureContinuation.<Object>call(c -> past.storage.getObject(id, c)));
  }

  @Test
  @DisplayName("a replication fetch whose reply carries another id leaves the object under that id alone")
  void replicationFetchWithOtherIdDoesNotOverwrite() throws Exception {
    DistPastTestContent legit = new DistPastTestContent(ring.env, idf, "legit");
    DistPastTestContent evil = new DistPastTestContent(legit.getId(), "evil", 0);
    Id requested = idf.buildId("requested");
    PastImpl past = bootReplying(evil);
    put(past, legit);

    Boolean fetched = await(FutureContinuation.<Boolean>call(c -> past.fetch(requested, null, c)));

    assertFalse(fetched, "a reply for another id counts as a successful fetch");
    assertEquals("legit", ((DistPastTestContent) stored(past, legit.getId())).getContent());
    assertFalse(past.storage.exists(requested), "stored under the requested id");
  }

  @Test
  @DisplayName("a replication fetch whose reply is not PastContent is dropped")
  void replicationFetchOfNonContentIsDropped() throws Exception {
    PastImpl past = bootReplying("not content");
    Id requested = idf.buildId("requested");

    Boolean fetched = await(FutureContinuation.<Boolean>call(c -> past.fetch(requested, null, c)));

    assertFalse(fetched);
    assertFalse(past.storage.exists(requested));
  }

  @Test
  @DisplayName("a replication fetch whose reply carries the requested id stores it")
  void replicationFetchOfRequestedIdIsStored() throws Exception {
    DistPastTestContent good = new DistPastTestContent(ring.env, idf, "good");
    PastImpl past = bootReplying(good);

    Boolean fetched = await(FutureContinuation.<Boolean>call(c -> past.fetch(good.getId(), null, c)));

    assertTrue(fetched);
    assertEquals("good", ((DistPastTestContent) stored(past, good.getId())).getContent());
  }
}
