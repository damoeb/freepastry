package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.past.testing.DistPastTestContent;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;

/**
 * Where and how PastImpl delivers its results: on the selector thread, with the exception the operation failed
 * with, and under the simulator as well as over sockets.
 */
class PastDeliveryTest {
  private static final long SECONDS = 10;

  private SocketRing ring;
  private Environment directEnvironment;

  @AfterEach
  void tearDown() {
    if (ring != null) ring.close();
    if (directEnvironment != null) directEnvironment.destroy();
  }

  /** The outcome of an operation, and whether it arrived on the selector thread. */
  record Delivery(Object outcome, boolean isOnSelector) {
  }

  private static Delivery deliver(Environment env, Consumer<Continuation<Object, Exception>> operation) throws Exception {
    CompletableFuture<Delivery> delivery = new CompletableFuture<Delivery>();
    env.getSelectorManager().invoke(() -> operation.accept(new Continuation<Object, Exception>() {
      @Override
      public void receiveResult(Object result) {
        delivery.complete(new Delivery(result, env.getSelectorManager().isSelectorThread()));
      }

      @Override
      public void receiveException(Exception exception) {
        delivery.complete(new Delivery(exception, env.getSelectorManager().isSelectorThread()));
      }
    }));
    return delivery.get(SECONDS, TimeUnit.SECONDS);
  }

  private static PastImpl newPast(PastryNode node, PastryIdFactory idFactory) {
    PastImpl past = new PastImpl(node, new CachedStorageImpl(idFactory, new MemoryStorage(idFactory), new NoOpCache(idFactory)), 1, "delivery");
    past.setContentDeserializer((buf, endpoint, type) -> new DistPastTestContent(buf, endpoint));
    return past;
  }

  /** A Past for {@link GCPastInsertTest.Content}, which has the same type number as DistPastTestContent. */
  private static PastImpl newRefusingPast(PastryNode node, PastryIdFactory idFactory) {
    PastImpl past = new PastImpl(node, new CachedStorageImpl(idFactory, new MemoryStorage(idFactory), new NoOpCache(idFactory)), 1, "delivery");
    past.setContentDeserializer((buf, endpoint, type) -> new GCPastInsertTest.Content(buf, endpoint));
    return past;
  }

  @Test
  @DisplayName("insert, lookup, lookupHandles and fetch deliver on the selector thread")
  void operationsDeliverOnTheSelectorThread() throws Exception {
    ring = new SocketRing();
    PastryIdFactory idFactory = new PastryIdFactory(ring.env);
    List<PastImpl> pasts = ring.boot(3, node -> newPast(node, idFactory));
    PastImpl local = pasts.get(0);
    DistPastTestContent content = new DistPastTestContent(ring.env, idFactory, "delivered");

    Delivery inserted = deliver(ring.env, continuation -> local.insert(content, continuation));
    Delivery found = deliver(ring.env, continuation -> pasts.get(1).lookup(content.getId(), false, continuation));
    Delivery handles = deliver(ring.env, continuation -> local.lookupHandles(content.getId(), 2, continuation));

    assertEquals(new Delivery(true, true), new Delivery(((Boolean[]) inserted.outcome())[0], inserted.isOnSelector()));
    assertEquals(new Delivery("delivered", true), new Delivery(((DistPastTestContent) found.outcome()).getContent(), found.isOnSelector()));
    assertTrue(handles.isOnSelector());
    PastContentHandle[] handleArray = (PastContentHandle[]) handles.outcome();
    assertEquals(2, handleArray.length);
    PastContentHandle handle = handleArray[0] != null ? handleArray[0] : handleArray[1];
    assertNotNull(handle, "no replica answered with a handle");
    Delivery fetched = deliver(ring.env, continuation -> local.fetch(handle, continuation));
    assertEquals(new Delivery("delivered", true), new Delivery(((DistPastTestContent) fetched.outcome()).getContent(), fetched.isOnSelector()));
    ring.assertNoExceptions();
  }

  @Test
  @DisplayName("an insert that every replica refuses fails with a PastException, on the selector thread")
  void refusedInsertFailsWithPastException() throws Exception {
    ring = new SocketRing();
    PastryIdFactory idFactory = new PastryIdFactory(ring.env);
    List<PastImpl> pasts = ring.boot(3, node -> newRefusingPast(node, idFactory));

    Delivery refused = deliver(ring.env, continuation -> pasts.get(0).insert(new GCPastInsertTest.Content(idFactory.buildId("refused"), true), continuation));

    assertTrue(refused.isOnSelector());
    assertSame(PastException.class, refused.outcome().getClass(), String.valueOf(refused.outcome()));
  }

  @Test
  @DisplayName("a request that gets no answer fails with a PastException after the message timeout, on the selector thread")
  void unansweredRequestFailsWithPastException() throws Exception {
    ring = new SocketRing();
    ring.env.getParameters().setInt("p2p_past_messageTimeout", 1000);
    PastryIdFactory idFactory = new PastryIdFactory(ring.env);
    List<PastImpl> pasts = ring.boot(3, node -> newPast(node, idFactory));
    rice.p2p.commonapi.NodeHandle gone = pasts.get(2).getLocalNodeHandle();
    ring.nodes.get(2).destroy();

    Delivery lost = deliver(ring.env, continuation -> pasts.get(0).lookupHandle(idFactory.buildId("lost"), gone, continuation));

    assertTrue(lost.isOnSelector());
    assertSame(PastException.class, lost.outcome().getClass(), String.valueOf(lost.outcome()));
  }

  @Test
  @DisplayName("under the simulator, an insert and a lookup on a single node deliver on the selector thread")
  void simulatorDeliversOnTheSelectorThread() throws Exception {
    directEnvironment = Environment.directEnvironment(1);
    PastryIdFactory idFactory = new PastryIdFactory(directEnvironment);
    PastryNode node = new DirectPastryNodeFactory(new RandomNodeIdFactory(directEnvironment),
        new EuclideanNetwork<DirectNodeHandle, RawMessage>(directEnvironment), directEnvironment).newNode();
    PastImpl past = newPast(node, idFactory);
    node.boot(java.util.Collections.emptyList());
    awaitReady(node);
    DistPastTestContent content = new DistPastTestContent(directEnvironment, idFactory, "simulated");

    Delivery inserted = deliver(directEnvironment, continuation -> past.insert(content, continuation));
    Delivery found = deliver(directEnvironment, continuation -> past.lookup(content.getId(), false, continuation));

    assertEquals(new Delivery(true, true), new Delivery(((Boolean[]) inserted.outcome())[0], inserted.isOnSelector()));
    assertEquals(new Delivery("simulated", true), new Delivery(((DistPastTestContent) found.outcome()).getContent(), found.isOnSelector()));
  }

  private static void awaitReady(PastryNode node) throws InterruptedException, IOException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SECONDS);
    while (!node.isReady()) {
      if (System.nanoTime() > deadline) throw new IOException("node not ready");
      Thread.sleep(10);
    }
  }
}
