package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.FutureContinuation;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.past.gc.GCPast;
import rice.p2p.past.gc.GCPastContent;
import rice.p2p.past.gc.GCPastContentHandle;
import rice.p2p.past.gc.GCPastImpl;
import rice.p2p.past.gc.GCPastMetadata;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.EmptyCache;
import rice.persistence.MemoryStorage;
import rice.persistence.StorageManagerImpl;

/**
 * The insert path of GCPastImpl: the object is checked by its own checkInsert, then stored with the expiration of the
 * insert as its metadata.
 */
class GCPastInsertTest {
  private static final long SECONDS = 10;
  private static final long EXPIRATION = 1_000_000_000_000L;

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

  private GCPastImpl newPast(PastryNode node) {
    GCPastImpl past = new GCPastImpl(node, new StorageManagerImpl(idf, new MemoryStorage(idf), new EmptyCache(idf)), null, 1,
        "gc-insert", new PastPolicy.DefaultPastPolicy(), 100_000_000L, null);
    past.setContentDeserializer((buf, endpoint, type) -> new Content(buf, endpoint));
    past.setContentHandleDeserializer((buf, endpoint, type) -> {
      throw new IOException("no handles in this test");
    });
    return past;
  }

  private List<GCPastImpl> boot() throws Exception {
    return ring.boot(3, this::newPast);
  }

  @Test
  @DisplayName("an insert stores the object with the expiration of the insert as its metadata")
  void insertStoresTheObjectWithItsExpiration() throws Exception {
    List<GCPastImpl> pasts = boot();
    Content content = new Content(idf.buildId("gc"), false);

    Boolean[] result = FutureContinuation.<Boolean[]>call(c -> pasts.get(0).insert(content, EXPIRATION, c)).get(SECONDS, TimeUnit.SECONDS);

    assertTrue(result[0]);
    int holders = 0;
    for (GCPastImpl past : pasts) {
      if (past.storage.exists(content.getId())) {
        holders++;
        assertEquals(new GCPastMetadata(EXPIRATION), past.storage.getMetadata(content.getId()));
      }
    }
    assertTrue(holders > 0, "no node stored the object");
  }

  @Test
  @DisplayName("an object whose checkInsert refuses is not stored")
  void insertRefusedByCheckInsertStoresNothing() throws Exception {
    List<GCPastImpl> pasts = boot();
    Content content = new Content(idf.buildId("refused"), true);

    assertThrows(ExecutionException.class,
        () -> FutureContinuation.<Boolean[]>call(c -> pasts.get(0).insert(content, EXPIRATION, c)).get(SECONDS, TimeUnit.SECONDS));

    for (GCPastImpl past : pasts) {
      assertFalse(past.storage.exists(content.getId()), "refused object stored");
    }
  }

  /** An object that refuses every insert when it was built to. */
  static class Content implements GCPastContent {
    private final Id id;
    private final boolean refuse;

    Content(Id id, boolean refuse) {
      this.id = id;
      this.refuse = refuse;
    }

    Content(InputBuffer buf, Endpoint endpoint) throws IOException {
      id = endpoint.readId(buf, buf.readShort());
      refuse = buf.readBoolean();
    }

    @Override
    public PastContent checkInsert(Id id, PastContent existingContent) throws PastException {
      if (refuse) {
        throw new PastException("refused");
      }
      return this;
    }

    @Override
    public PastContentHandle getHandle(Past local) {
      throw new UnsupportedOperationException();
    }

    @Override
    public GCPastContentHandle getHandle(GCPast local, long expiration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public GCPastMetadata getMetadata(long expiration) {
      return new GCPastMetadata(expiration);
    }

    @Override
    public Id getId() {
      return id;
    }

    @Override
    public boolean isMutable() {
      return true;
    }

    @Override
    public long getVersion() {
      return 0;
    }

    @Override
    public short getType() {
      return 1;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeShort(id.getType());
      id.serialize(buf);
      buf.writeBoolean(refuse);
    }
  }
}
