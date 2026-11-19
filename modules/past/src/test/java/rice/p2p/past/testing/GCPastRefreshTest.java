package rice.p2p.past.testing;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.testing.CommonAPITest;
import rice.p2p.commonapi.testing.RegressionRun;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.past.ObjectNotFoundException;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.PastException;
import rice.p2p.past.PastPolicy;
import rice.p2p.past.gc.ExpiringPast;
import rice.p2p.past.gc.ExpiringPastContent;
import rice.p2p.past.gc.ExpiringPastContentHandle;
import rice.p2p.past.gc.ExpiringPastImpl;
import rice.p2p.past.gc.ExpiringPastMetadata;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Refreshes objects through {@link ExpiringPastImpl} on a ring of the direct simulator.
 */
@Tag("integration")
class GCPastRefreshTest {
  private static final int RING_SIZE = 5;

  @Test
  @DisplayName("refresh reports success per existing id and ObjectNotFoundException per missing id")
  void refreshReportsPerId() throws Exception {
    RegressionRun.run(env -> {
      env.getParameters().setInt("commonapi_testing_num_nodes", RING_SIZE);
      return new RefreshRing(env);
    });
  }

  /** The ring and its steps. */
  static class RefreshRing extends CommonAPITest {
    private static final int REPLICAS = 3;
    private static final long AWAIT_TIMEOUT_MS = 30_000;
    private static final long COLLECTION_INTERVAL = 100_000_000;
    private static final long FAR_FUTURE = 1_000_000_000_000L;

    private final ExpiringPastImpl[] pasts;

    RefreshRing(Environment env) throws IOException {
      super(env);
      pasts = new ExpiringPastImpl[NUM_NODES];
    }

    @Override
    protected void processNode(int num, Node node) {
      pasts[num] = new ExpiringPastImpl(node, new CachedStorageImpl(FACTORY, new MemoryStorage(FACTORY), new NoOpCache(FACTORY)),
          null, REPLICAS, "gc-refresh-test", new PastPolicy.DefaultPastPolicy(), COLLECTION_INTERVAL, null);
      pasts[num].setContentDeserializer((buf, endpoint, type) -> new Content(buf, endpoint));
      pasts[num].setContentHandleDeserializer((buf, endpoint, type) -> {
        throw new IOException("no handles in this test");
      });
    }

    @Override
    protected void runTest() {
      ExpiringPastImpl local = pasts[0];
      Id existing = FACTORY.buildId("existing");
      Id missing = FACTORY.buildId("missing");

      sectionStart("ExpiringPast refresh");

      stepStart("Insert");
      Object inserted = await(continuation -> local.insert(new Content(existing), FAR_FUTURE / 2, continuation));
      assertTrue("insert should succeed", inserted instanceof Boolean[]);
      stepDone();

      stepStart("Refresh of an existing id");
      Object[] result = refresh(local, new Id[] {existing}, FAR_FUTURE);
      assertEquals("one result per id", 1, result.length);
      assertEquals("existing id is refreshed", true, result[0]);
      stepDone();

      stepStart("Refresh of a missing id");
      result = refresh(local, new Id[] {missing}, FAR_FUTURE);
      assertTrue("missing id yields ObjectNotFoundException, got " + result[0], result[0] instanceof ObjectNotFoundException);
      stepDone();

      stepStart("Refresh to an earlier expiration than the current one");
      result = refresh(local, new Id[] {existing}, FAR_FUTURE / 4);
      assertEquals("earlier expiration still succeeds", true, result[0]);
      stepDone();

      stepStart("Refresh of existing and missing ids together");
      result = refresh(local, new Id[] {missing, existing}, FAR_FUTURE);
      assertEquals("one result per id", 2, result.length);
      assertTrue("first is ObjectNotFoundException, got " + result[0], result[0] instanceof ObjectNotFoundException);
      assertEquals("second is refreshed", true, result[1]);
      stepDone();

      sectionDone();
    }

    private Object[] refresh(ExpiringPast past, Id[] ids, long expiration) {
      long[] expirations = new long[ids.length];
      java.util.Arrays.fill(expirations, expiration);
      Object result = await(continuation -> past.refresh(ids, expirations, continuation));
      return assertInstanceOf(Object[].class, result);
    }

    private Object await(Consumer<Continuation<Object, Exception>> operation) {
      CompletableFuture<Object> future = new CompletableFuture<>();
      operation.accept(new Continuation<Object, Exception>() {
        @Override
        public void receiveResult(Object result) {
          future.complete(result);
        }

        @Override
        public void receiveException(Exception exception) {
          future.completeExceptionally(exception);
        }
      });
      try {
        return future.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        throw new AssertionError("operation did not complete: " + e, e);
      }
    }
  }

  /** An object with a fixed version, stored under its id. */
  static class Content implements ExpiringPastContent {
    private final Id id;

    Content(Id id) {
      this.id = id;
    }

    Content(InputBuffer buf, Endpoint endpoint) throws IOException {
      id = endpoint.readId(buf, buf.readShort());
    }

    @Override
    public PastContent checkInsert(Id id, PastContent existingContent) throws PastException {
      return this;
    }

    @Override
    public PastContentHandle getHandle(Past local) {
      throw new UnsupportedOperationException();
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
    public ExpiringPastContentHandle getHandle(ExpiringPast local, long expiration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ExpiringPastMetadata getMetadata(long expiration) {
      return new ExpiringPastMetadata(expiration);
    }

    @Override
    public short getType() {
      return 1;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeShort(id.getType());
      id.serialize(buf);
    }
  }
}
