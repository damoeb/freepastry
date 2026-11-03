package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.slf4j.LoggerFactory;
import rice.environment.logging.Slf4jLogManager;
import rice.environment.time.simple.SimpleTimeSource;
import rice.selector.SelectorManager;

class EntityManagerConcurrentSendTest {
  private static final int THREADS = 8;
  private static final int PER_THREAD = 500;
  private static final int MAX_MSG_SIZE = 10;
  private static final int MAX_BIG_MSG_SIZE = 1024;

  private final AtomicInteger openedSockets = new AtomicInteger();

  @SuppressWarnings("unchecked")
  private EntityManager<String> target() {
    TransportLayer<String, ByteBuffer> tl = (TransportLayer<String, ByteBuffer>) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {TransportLayer.class}, (proxy, method, args) -> {
          if (method.getName().equals("openSocket")) openedSockets.incrementAndGet();
          return null;
        });
    LivenessProvider<String> alive = (LivenessProvider<String>) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {LivenessProvider.class}, (proxy, method, args) -> 0);
    // never started: not the selector thread for anybody, and what is invoked on it is never run
    SelectorManager selector = new SelectorManager("test", new SimpleTimeSource(), new Slf4jLogManager());
    EntityManagerContext<String> ctx = new EntityManagerContext<>(tl, alive, null, selector,
        LoggerFactory.getLogger(EntityManagerConcurrentSendTest.class), MAX_MSG_SIZE,
        THREADS * PER_THREAD, MAX_BIG_MSG_SIZE, List.of(), new TransportListeners<>(), () -> null, () -> null,
        () -> false, id -> null);
    return new EntityManager<>("peer", ctx);
  }

  private List<MessageRequestHandle<String, ByteBuffer>> sendFromThreads(EntityManager<String> target, int size)
      throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<List<MessageRequestHandle<String, ByteBuffer>>>> futures = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        futures.add(pool.submit(() -> {
          start.await();
          List<MessageRequestHandle<String, ByteBuffer>> handles = new ArrayList<>();
          for (int i = 0; i < PER_THREAD; i++) {
            handles.add(target.send("peer", ByteBuffer.allocate(size), null, null));
          }
          return handles;
        }));
      }
      start.countDown();
      List<MessageRequestHandle<String, ByteBuffer>> all = new ArrayList<>();
      for (Future<List<MessageRequestHandle<String, ByteBuffer>>> f : futures) {
        all.addAll(f.get(30, TimeUnit.SECONDS));
      }
      return all;
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  @DisplayName("concurrent off-thread sends get distinct, gap-free sequence numbers")
  void concurrentSendsKeepSequenceNumbersUnique() throws Exception {
    EntityManager<String> target = target();

    List<MessageRequestHandle<String, ByteBuffer>> handles = sendFromThreads(target, MAX_MSG_SIZE);

    Set<Integer> seqs = new HashSet<>();
    for (MessageRequestHandle<String, ByteBuffer> h : handles) {
      seqs.add(((MessageWrapper<?>) h).seq);
    }
    int total = THREADS * PER_THREAD;
    assertEquals(total, handles.size());
    assertEquals(total, seqs.size());
    assertEquals(Integer.MIN_VALUE, seqs.stream().mapToInt(Integer::intValue).min().getAsInt());
    assertEquals(Integer.MIN_VALUE + total - 1, seqs.stream().mapToInt(Integer::intValue).max().getAsInt());
    assertEquals(total, target.queue.size());
  }

  @Test
  @DisplayName("concurrent off-thread big messages to one peer share a single sender and a single socket")
  void concurrentBigSendsShareOneSender() throws Exception {
    EntityManager<String> target = target();

    List<MessageRequestHandle<String, ByteBuffer>> handles = sendFromThreads(target, MAX_MSG_SIZE + 1);

    assertEquals(THREADS * PER_THREAD, handles.size());
    assertEquals(1, target.pendingBigMessages.size());
    assertEquals(1, openedSockets.get());
  }
}
