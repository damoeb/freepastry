package rice.pastry.commonapi;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.PastryNode;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.standard.RandomNodeIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link Endpoint#process}: under the direct simulator its future completes on the selector thread, with the
 * task's result or the exception it threw.
 */
class PastryEndpointProcessTest {

  static final long WAIT_MILLIS = 5000;

  private static final Application APPLICATION = new Application() {
    @Override
    public boolean forward(RouteMessage message) {
      return true;
    }

    @Override
    public void deliver(Id id, Message message) {
    }

    @Override
    public void update(NodeHandle handle, boolean joined) {
    }
  };

  private Environment env;

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private Endpoint endpoint() throws Exception {
    env = Environment.directEnvironment(1);
    PastryNode node = new DirectPastryNodeFactory(new RandomNodeIdFactory(env),
        new EuclideanNetwork<DirectNodeHandle, RawMessage>(env), env).newNode();
    return node.buildEndpoint(APPLICATION, "process");
  }

  /** The outcome, and whether it arrived on the selector thread. */
  record Delivery(Object outcome, boolean onSelector) {
  }

  @Test
  void theFutureCompletesOnTheSelectorThread() throws Exception {
    Endpoint endpoint = endpoint();
    CompletableFuture<Delivery> result = new CompletableFuture<Delivery>();
    CompletableFuture<Delivery> failure = new CompletableFuture<Delivery>();
    IOException taskFailure = new IOException("task failed");
    env.getSelectorManager().invoke(() -> {
      var _ = endpoint.process(() -> "done")
          .whenComplete((value, throwable) -> result.complete(new Delivery(value, env.getSelectorManager().isSelectorThread())));
      var _ = endpoint.<String>process(() -> {
        throw taskFailure;
      }).whenComplete((value, throwable) -> failure.complete(new Delivery(Futures.unwrap(throwable), env.getSelectorManager().isSelectorThread())));
    });
    assertEquals(new Delivery("done", true), result.get(WAIT_MILLIS, TimeUnit.MILLISECONDS));
    assertEquals(new Delivery(taskFailure, true), failure.get(WAIT_MILLIS, TimeUnit.MILLISECONDS));
  }
}
