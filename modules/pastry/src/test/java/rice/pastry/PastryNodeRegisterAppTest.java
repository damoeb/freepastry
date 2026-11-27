package rice.pastry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.client.PastryAppl;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.messaging.Message;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * An app that registers off the selector thread while the node goes ready must be told so exactly once: neither
 * by both {@code registerApp} and the observers' notification, nor by neither of them.
 */
class PastryNodeRegisterAppTest {

  private static final long TIMEOUT_SECONDS = 10;
  private static final int APP_ADDRESS = 4711;

  private Environment env;
  private PastryNode node;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    node = new DirectPastryNodeFactory(new RandomNodeIdFactory(env),
        new EuclideanNetwork<DirectNodeHandle, RawMessage>(env), env).newNode();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private void runOnSelector(Runnable task) throws Exception {
    CompletableFuture<Void> done = new CompletableFuture<Void>();
    env.getSelectorManager().invoke(() -> {
      try {
        task.run();
        done.complete(null);
      } catch (Throwable t) {
        done.completeExceptionally(t);
      }
    });
    done.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("an app that sees the node not ready yet while it goes ready is still notified once")
  void appRegisteredWhileTheNodeGoesReadyIsNotified() throws Exception {
    RacingReadyStrategy strategy = new RacingReadyStrategy(false);
    runOnSelector(() -> node.setReadyStrategy(strategy));
    CountingApp app = new CountingApp(node);

    node.registerApp(app);
    runOnSelector(() -> node.setReady(true));
    runOnSelector(() -> { });

    assertEquals(1, app.readyNotifications.get());
  }

  @Test
  @DisplayName("an app that sees the node ready before the observers are told is notified once, not twice")
  void appRegisteredBeforeTheObserversAreToldIsNotNotifiedTwice() throws Exception {
    RacingReadyStrategy strategy = new RacingReadyStrategy(true);
    runOnSelector(() -> node.setReadyStrategy(strategy));
    CountingApp app = new CountingApp(node);

    node.registerApp(app);
    runOnSelector(() -> node.setReady(true));
    runOnSelector(() -> { });

    assertEquals(1, app.readyNotifications.get());
  }

  @Test
  @DisplayName("an app registered off the selector thread is notified on the selector thread")
  void appRegisteredOffTheSelectorIsNotifiedOnTheSelector() throws Exception {
    RacingReadyStrategy strategy = new RacingReadyStrategy(false);
    runOnSelector(() -> {
      node.setReadyStrategy(strategy);
      node.setReady(true);
    });
    CountingApp app = new CountingApp(node);

    node.registerApp(app);
    runOnSelector(() -> { });

    assertEquals(1, app.readyNotifications.get());
    assertTrue(app.wasNotifiedOnTheSelector, "notifyReady() ran off the selector thread");
  }

  /**
   * Holds the committed ready state like {@code PeriodicLeafSetProtocol}, but its first query off the selector
   * thread answers from a lease view that disagrees with it, as a query racing the selector can.
   */
  private final class RacingReadyStrategy implements ReadyStrategy {
    private volatile boolean isReady = false;
    private volatile boolean hasRaced = false;
    private final boolean doesTheRacingQuerySeeReady;

    RacingReadyStrategy(boolean doesTheRacingQuerySeeReady) {
      this.doesTheRacingQuerySeeReady = doesTheRacingQuerySeeReady;
    }

    @Override
    public void setReady(boolean ready) {
      if (ready != isReady) {
        isReady = ready;
        node.notifyReadyObservers();
      }
    }

    @Override
    public boolean isReady() {
      if (env.getSelectorManager().isSelectorThread() || hasRaced) {
        return isReady;
      }
      hasRaced = true;
      if (!doesTheRacingQuerySeeReady) {
        // the selector completes the transition between this answer and the caller acting on it
        try {
          runOnSelector(() -> setReady(true));
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
      return doesTheRacingQuerySeeReady;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
  }

  private final class CountingApp extends PastryAppl {
    final AtomicInteger readyNotifications = new AtomicInteger();
    volatile boolean wasNotifiedOnTheSelector = true;

    CountingApp(PastryNode node) {
      super(node, null, APP_ADDRESS, null);
    }

    @Override
    public void notifyReady() {
      readyNotifications.incrementAndGet();
      wasNotifiedOnTheSelector &= env.getSelectorManager().isSelectorThread();
    }

    @Override
    public void messageForApplication(Message msg) {
    }
  }
}
