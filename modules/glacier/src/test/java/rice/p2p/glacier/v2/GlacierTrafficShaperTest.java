package rice.p2p.glacier.v2;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import rice.Continuation;
import rice.environment.Environment;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlacierTrafficShaperTest {

  private Environment env;
  private DirectTimeSource time;
  private PastryIdFactory idFactory;
  private GlacierTrafficShaper testObj;
  private List<Id> released;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    time = (DirectTimeSource) env.getTimeSource();
    idFactory = new PastryIdFactory(env);
    testObj = new GlacierTrafficShaper(GlacierConfig.fromParameters(env.getParameters()), 48, env, LoggerFactory.getLogger(GlacierTrafficShaperTest.class));
    released = new ArrayList<Id>();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private Continuation<Object, Exception> job(final Id key) {
    return new Continuation.SimpleContinuation() {
      @Override
      public void receiveResult(Object result) {
        released.add(key);
      }
    };
  }

  @Test
  void enqueueReturnsTheJobItReplaces() {
    Id key = idFactory.buildId("key");
    Continuation<Object, Exception> first = job(key);

    testObj.enqueue(key, first);
    Continuation<Object, Exception> actual = testObj.enqueue(key, job(key));

    assertSame(first, actual);
    assertTrue(testObj.isPending(key));
    assertEquals(1, testObj.numPending());
  }

  @Test
  void shaperTaskReleasesQueuedJobsOnceTokensHaveAccumulated() {
    Id key = idFactory.buildId("key");
    testObj.enqueue(key, job(key));
    GlacierContinuation task = testObj.newShaperTask();
    task.init();

    time.incrementTime(1000);
    task.timeoutExpired();

    assertEquals(List.of(key), released);
    assertFalse(testObj.isPending(key));
  }

  @Test
  void shaperTaskHoldsJobsBackWhileTooManyRestoresAreActive() {
    Id key = idFactory.buildId("key");
    testObj.enqueue(key, job(key));
    for (int i = 0; i < env.getParameters().getInt("p2p_glacier_maxActiveRestores"); i++)
      testObj.restoreStarted();
    GlacierContinuation task = testObj.newShaperTask();
    task.init();

    time.incrementTime(1000);
    task.timeoutExpired();

    assertTrue(released.isEmpty());
    assertTrue(testObj.isPending(key));
  }

  @Test
  void shaperTaskReschedulesItselfLaterWhenIdle() {
    GlacierContinuation task = testObj.newShaperTask();
    task.init();
    long before = task.getTimeout();

    time.incrementTime(1000);
    task.timeoutExpired();

    assertTrue(task.getTimeout() > before);
  }
}
