package rice.p2p.glacier.v2;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import rice.environment.Environment;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlacierSchedulerTest {

  /** A scheduled timeout message: its delay and whether it was cancelled. */
  private static final class Timer implements CancellableTask {
    final long delay;
    boolean cancelled;

    Timer(long delay) {
      this.delay = delay;
    }

    @Override
    public boolean cancel() {
      cancelled = true;
      return true;
    }

    @Override
    public void run() {}

    @Override
    public long scheduledExecutionTime() {
      return 0;
    }
  }

  /** A continuation whose timeout the test controls. */
  private static final class FixedTimeout extends GlacierContinuation {
    long timeout;

    FixedTimeout(long timeout) {
      this.timeout = timeout;
    }

    @Override
    public void receiveResult(Object result) {}

    @Override
    public void receiveException(Exception exception) {}

    @Override
    public void timeoutExpired() {
      timeout += 60 * GlacierConfig.SECONDS;
    }

    @Override
    public long getTimeout() {
      return timeout;
    }
  }

  private Environment env;
  private DirectTimeSource time;
  private List<Timer> timers;
  private GlacierContinuationRegistry testObj;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    time = (DirectTimeSource) env.getTimeSource();
    timers = new ArrayList<>();
    Id localId = new PastryIdFactory(env).buildId("local");
    Endpoint endpoint = (Endpoint) Proxy.newProxyInstance(Endpoint.class.getClassLoader(), new Class<?>[] {Endpoint.class}, (proxy, method, args) -> {
      if (method.getName().equals("scheduleMessage")) {
        Timer timer = new Timer((Long) args[1]);
        timers.add(timer);
        return timer;
      }
      if (method.getName().equals("getLocalNodeHandle")) {
        return new NodeHandle() {
          @Override
          public Id getId() {
            return localId;
          }

          @Override
          public boolean checkLiveness() {
            return true;
          }

          @Override
          public void serialize(OutputBuffer buf) {}
        };
      }
      return null;
    });
    testObj = new GlacierContinuationRegistry(endpoint, env, LoggerFactory.getLogger(GlacierSchedulerTest.class));
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private long activeTimers() {
    return timers.stream().filter(t -> !t.cancelled).count();
  }

  @Test
  void anEarlierContinuationRearmsTheTimerAfterTheTimerHasFired() {
    long now = time.currentTimeMillis();
    testObj.addContinuation(new FixedTimeout(now + 5000));
    time.incrementTime(5000);
    testObj.timerExpired();

    testObj.addContinuation(new FixedTimeout(time.currentTimeMillis() + 20000));

    Timer last = timers.get(timers.size() - 1);
    assertEquals(20000, last.delay);
    assertEquals(1, activeTimers());
  }

  @Test
  void expiringTheTimerNeverLeavesTwoTimerChainsRunning() {
    long now = time.currentTimeMillis();
    testObj.addContinuation(new FixedTimeout(now + 5000));
    testObj.timerExpired();
    testObj.timerExpired();

    assertEquals(1, activeTimers());
  }

  @Test
  void aContinuationThatDoesNotMoveItsTimeoutIsTerminatedAndTheOthersKeepTheirTimer() {
    long now = time.currentTimeMillis();
    GlacierContinuation stuck = new GlacierContinuation() {
      @Override
      public void receiveResult(Object result) {}

      @Override
      public void receiveException(Exception exception) {}

      @Override
      public void timeoutExpired() {
        // a local bug: neither terminates nor sets a later timeout
      }

      @Override
      public long getTimeout() {
        return now + 5000;
      }
    };
    FixedTimeout healthy = new FixedTimeout(now + 5000);
    testObj.addContinuation(stuck);
    testObj.addContinuation(healthy);
    time.incrementTime(5000);

    testObj.timerExpired();

    assertTrue(stuck.hasTerminated(), "the stuck continuation is terminated");
    assertEquals(1, testObj.size(), "only the stuck continuation is removed");
    assertEquals(1, activeTimers(), "the timer is armed for the healthy continuation");
  }
}
