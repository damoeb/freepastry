package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.FutureContinuation;
import rice.p2p.past.testing.DistPastTestContent;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;
import rice.persistence.StorageManagerImpl;

/**
 * Past on a real socket ring on loopback: inserts and lookups are routed as bytes, replicas are stored on other nodes
 * and contents and handles come back over the wire. The direct simulator hands the message objects over, so only a
 * ring like this one runs the reading and writing code on the way.
 */
class PastSocketRingTest {
  private static final int NODES = 6;
  private static final int REPLICAS = 2;
  private static final long SECONDS = 10;
  private static final int CACHE_BYTES = 512 * 1024;

  private SocketRing ring;

  @BeforeEach
  void setUp() {
    ring = new SocketRing();
  }

  @AfterEach
  void tearDown() {
    ring.close();
  }

  private static <R> R await(java.util.concurrent.CompletableFuture<R> future) throws Exception {
    return future.get(SECONDS, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("on a six node socket ring contents inserted at one node are stored on every replica and found from all")
  void insertAndLookup() throws Exception {
    PastryIdFactory idf = new PastryIdFactory(ring.env);
    List<Past> pasts = ring.boot(NODES, node -> {
      PastImpl past = new PastImpl(node, new StorageManagerImpl(idf, new MemoryStorage(idf),
          new LRUCache(new MemoryStorage(idf), CACHE_BYTES, node.getEnvironment())), REPLICAS, "socket-ring");
      past.setContentDeserializer((buf, endpoint, type) -> {
        if (type == DistPastTestContent.TYPE) {
          return new DistPastTestContent(buf, endpoint);
        }
        throw new IOException("Unknown content type " + type);
      });
      return past;
    });

    for (int i = 0; i < NODES; i++) {
      DistPastTestContent content = new DistPastTestContent(ring.env, idf, "content " + i);
      Past inserter = pasts.get(i);
      Boolean[] stored = await(FutureContinuation.<Boolean[]>call(c -> inserter.insert(content, c)));
      assertEquals(REPLICAS + 1, stored.length, "replicas asked to store content " + i);
      assertEquals(REPLICAS + 1, Arrays.stream(stored).filter(Boolean::booleanValue).count(),
          "replicas that stored content " + i + ": " + Arrays.toString(stored));

      for (int j = 0; j < NODES; j++) {
        Past at = pasts.get((i + j + 1) % NODES);
        PastContent found = await(FutureContinuation.<PastContent>call(c -> at.lookup(content.getId(), false, c)));
        assertNotNull(found, "content " + i + " found from node " + j);
        assertEquals(content.getContent(), ((DistPastTestContent) found).getContent());
      }

      Past other = pasts.get((i + 3) % NODES);
      PastContentHandle[] handles = await(FutureContinuation.<PastContentHandle[]>call(
          c -> other.lookupHandles(content.getId(), REPLICAS + 1, c)));
      assertEquals(REPLICAS + 1, Arrays.stream(handles).filter(h -> h != null).count(),
          "handles of content " + i + ": " + Arrays.toString(handles));
      PastContent fetched = await(FutureContinuation.<PastContent>call(c -> other.fetch(handles[0], c)));
      assertEquals(content.getContent(), ((DistPastTestContent) fetched).getContent());
    }
    ring.assertNoExceptions();
  }
}
