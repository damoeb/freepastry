package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.slf4j.LoggerFactory;
import rice.environment.logging.Slf4jLogManager;
import rice.environment.time.simple.SimpleTimeSource;
import rice.selector.SelectorManager;

/** The bounded write queue of an EntityManager: priority order, drop on overflow, cancel, purge. */
class EntityManagerQueueTest {
  private static final int MAX_QUEUE_SIZE = 3;

  private final List<MessageRequestHandle<String, ByteBuffer>> failed = new ArrayList<>();
  private final List<Exception> reasons = new ArrayList<>();

  private final MessageCallback<String, ByteBuffer> recorder = new MessageCallback<>() {
    @Override
    public void ack(MessageRequestHandle<String, ByteBuffer> msg) {
    }

    @Override
    public void sendFailed(MessageRequestHandle<String, ByteBuffer> msg, Exception reason) {
      failed.add(msg);
      reasons.add(reason);
    }
  };

  @SuppressWarnings("unchecked")
  private EntityManager<String> target() {
    TransportLayer<String, ByteBuffer> tl = (TransportLayer<String, ByteBuffer>) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {TransportLayer.class}, (proxy, method, args) -> null);
    LivenessProvider<String> alive = (LivenessProvider<String>) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {LivenessProvider.class}, (proxy, method, args) -> 0);
    // never started: not the selector thread for anybody, and what is invoked on it is never run
    SelectorManager selector = new SelectorManager("test", new SimpleTimeSource(), new Slf4jLogManager());
    EntityManagerContext<String> ctx = new EntityManagerContext<>(tl, alive, null, selector,
        LoggerFactory.getLogger(EntityManagerQueueTest.class), 100, MAX_QUEUE_SIZE, 1000, List.of(),
        new TransportListeners<>(), () -> null, () -> null, () -> false, id -> null);
    return new EntityManager<>("peer", ctx);
  }

  private MessageRequestHandle<String, ByteBuffer> send(EntityManager<String> target, int priority) {
    return target.send("peer", ByteBuffer.allocate(1), recorder, Map.of(PriorityTransportLayer.OPTION_PRIORITY, priority));
  }

  @Test
  @DisplayName("the queue hands out the highest priority first, and the oldest among equals")
  void peekFollowsPriorityThenArrival() {
    EntityManager<String> target = target();
    MessageRequestHandle<String, ByteBuffer> low = send(target, 5);
    MessageRequestHandle<String, ByteBuffer> highFirst = send(target, 0);
    MessageRequestHandle<String, ByteBuffer> highSecond = send(target, 0);

    assertSame(highFirst, target.peek());
    assertTrue(highFirst.cancel());
    assertSame(highSecond, target.peek());
    assertTrue(highSecond.cancel());
    assertSame(low, target.peek());
  }

  @Test
  @DisplayName("a full queue drops the newest message of the lowest priority, even if it is older")
  void overflowDropsTheNewestOfTheLowestPriority() {
    EntityManager<String> target = target();
    MessageRequestHandle<String, ByteBuffer> lowOld = send(target, 5);
    MessageRequestHandle<String, ByteBuffer> lowNew = send(target, 5);
    send(target, 0);

    MessageRequestHandle<String, ByteBuffer> sameAsLowest = send(target, 5);
    send(target, 0);

    assertEquals(List.of(sameAsLowest, lowNew), failed);
    reasons.forEach(r -> assertInstanceOf(QueueOverflowException.class, r));
    assertFalse(lowNew.cancel());
    assertTrue(lowOld.cancel());
  }

  @Test
  @DisplayName("purge fails every queued message in queue order and leaves the manager idle")
  void purgeFailsEveryQueuedMessage() {
    EntityManager<String> target = target();
    MessageRequestHandle<String, ByteBuffer> a = send(target, 1);
    MessageRequestHandle<String, ByteBuffer> b = send(target, 0);

    target.purge(new IOException("boom"));

    assertEquals(List.of(b, a), failed);
    assertTrue(target.isIdle());
  }
}
