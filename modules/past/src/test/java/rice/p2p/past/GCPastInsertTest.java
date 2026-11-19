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
import rice.p2p.past.gc.ExpiringPast;
import rice.p2p.past.gc.ExpiringPastContent;
import rice.p2p.past.gc.ExpiringPastContentHandle;
import rice.p2p.past.gc.ExpiringPastImpl;
import rice.p2p.past.gc.ExpiringPastMetadata;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;

/**
 * The insert path of ExpiringPastImpl: the object is checked by its own checkInsert, then stored with the expiration of the
 * insert as its metadata.
 */
class GCPastInsertTest {
  private static final long SECONDS = 10;
  private static final long EXPIRATION = 1_000_000_000_000L;

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

  private ExpiringPastImpl newPast(PastryNode node) {
    ExpiringPastImpl past = new ExpiringPastImpl(node, new CachedStorageImpl(idFactory, new MemoryStorage(idFactory), new NoOpCache(idFactory)), null, 1,
        "gc-insert", new PastPolicy.DefaultPastPolicy(), 100_000_000L, null);
    past.setContentDeserializer((buf, endpoint, type) -> new Content(buf, endpoint));
    past.setContentHandleDeserializer((buf, endpoint, type) -> {
      throw new IOException("no handles in this test");
    });
    return past;
  }

  private List<ExpiringPastImpl> boot() throws Exception {
    return ring.boot(3, this::newPast);
  }

  @Test
  @DisplayName("an insert stores the object with the expiration of the insert as its metadata")
  void insertStoresTheObjectWithItsExpiration() throws Exception {
    List<ExpiringPastImpl> pasts = boot();
    Content content = new Content(idFactory.buildId("gc"), false);

    Boolean[] result = FutureContinuation.<Boolean[]>call(continuation -> pasts.get(0).insert(content, EXPIRATION, continuation)).get(SECONDS, TimeUnit.SECONDS);

    assertTrue(result[0]);
    int holders = 0;
    for (ExpiringPastImpl past : pasts) {
      if (past.storage.exists(content.getId())) {
        holders++;
        assertEquals(new ExpiringPastMetadata(EXPIRATION), past.storage.getMetadata(content.getId()));
      }
    }
    assertTrue(holders > 0, "no node stored the object");
  }

  @Test
  @DisplayName("an object whose checkInsert refuses is not stored")
  void insertRefusedByCheckInsertStoresNothing() throws Exception {
    List<ExpiringPastImpl> pasts = boot();
    Content content = new Content(idFactory.buildId("refused"), true);

    assertThrows(ExecutionException.class,
        () -> FutureContinuation.<Boolean[]>call(continuation -> pasts.get(0).insert(content, EXPIRATION, continuation)).get(SECONDS, TimeUnit.SECONDS));

    for (ExpiringPastImpl past : pasts) {
      assertFalse(past.storage.exists(content.getId()), "refused object stored");
    }
  }

  /** An object that refuses every insert when it was built to. */
  static class Content implements ExpiringPastContent {
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
    public ExpiringPastContentHandle getHandle(ExpiringPast local, long expiration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ExpiringPastMetadata getMetadata(long expiration) {
      return new ExpiringPastMetadata(expiration);
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
