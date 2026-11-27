package rice;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaCall;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Enforces the threading rules of {@link Futures} on the main code of every module (the tutorials module
 * depends on all of them): futures must stay on the selector thread and the simulator's virtual time.
 */
public class FutureThreadingRulesTest {

  private static final Set<String> FUTURE_TYPES = Set.of(
      "java.util.concurrent.CompletableFuture",
      "java.util.concurrent.CompletionStage",
      "java.util.concurrent.Future");

  private static final Set<String> REAL_TIME_METHODS = Set.of("orTimeout", "completeOnTimeout", "delayedExecutor");

  private static final Set<String> BLOCKING_METHODS = Set.of("get", "join");

  /** Main classes only: not the test classes of this module, nor the test-jars of others. */
  private static final ImportOption MAIN_CODE_ONLY = location -> !location.contains("/test-classes/")
      && !location.contains("-tests.jar");

  private static JavaClasses mainClasses;

  private static synchronized JavaClasses mainClasses() {
    if (mainClasses == null) {
      mainClasses = new ClassFileImporter().withImportOption(MAIN_CODE_ONLY).importPackages("rice", "org.mpisws");
    }
    return mainClasses;
  }

  private static boolean isFutureMethod(JavaCall<?> call) {
    return FUTURE_TYPES.contains(call.getTargetOwner().getName());
  }

  static final ArchRule NO_ASYNC_WITHOUT_EXECUTOR = noClasses()
      .should().callCodeUnitWhere(new DescribedPredicate<JavaCall<?>>(
          "an *Async method of a future without an Executor (it would run on ForkJoinPool.commonPool())") {
        @Override
        public boolean test(JavaCall<?> call) {
          return isFutureMethod(call)
              && call.getTarget().getName().endsWith("Async")
              && call.getTarget().getRawParameterTypes().stream().noneMatch(type -> type.isEquivalentTo(Executor.class));
        }
      })
      .because("dependent stages must run on the selector thread: pass SelectorLoop.executor()");

  static final ArchRule NO_REAL_TIME_TIMEOUTS = noClasses()
      .should().callCodeUnitWhere(new DescribedPredicate<JavaCall<?>>("orTimeout, completeOnTimeout or delayedExecutor") {
        @Override
        public boolean test(JavaCall<?> call) {
          return isFutureMethod(call) && REAL_TIME_METHODS.contains(call.getTarget().getName());
        }
      })
      .because("they complete on a real-time JDK thread; use Futures.timeout on the selector's timer");

  static final ArchRule NO_FORK_JOIN_POOL = noClasses()
      .should().dependOnClassesThat().haveFullyQualifiedName(ForkJoinPool.class.getName())
      .because("protocol code runs on the selector thread, never on the common pool");

  static final ArchRule BLOCK_ONLY_IN_AWAIT = noClasses()
      .should().callCodeUnitWhere(new DescribedPredicate<JavaCall<?>>("get() or join() of a future outside Futures.await") {
        @Override
        public boolean test(JavaCall<?> call) {
          JavaCodeUnit origin = call.getOrigin();
          boolean inAwait = origin.getOwner().isEquivalentTo(Futures.class) && origin.getName().equals("await");
          return isFutureMethod(call) && BLOCKING_METHODS.contains(call.getTarget().getName()) && !inAwait;
        }
      })
      .because("blocking on the selector thread deadlocks; Futures.await refuses to");

  @Test
  void noAsyncWithoutExecutor() {
    NO_ASYNC_WITHOUT_EXECUTOR.check(mainClasses());
  }

  @Test
  void noRealTimeTimeouts() {
    NO_REAL_TIME_TIMEOUTS.check(mainClasses());
  }

  @Test
  void noForkJoinPool() {
    NO_FORK_JOIN_POOL.check(mainClasses());
  }

  @Test
  void blockOnlyInAwait() {
    BLOCK_ONLY_IN_AWAIT.check(mainClasses());
  }

  @Test
  void theMainCodeWasImported() {
    assertTrue(mainClasses().contain(Futures.class), "rice.Futures");
    assertTrue(mainClasses().contain("rice.p2p.past.PastImpl"), "the past module");
    assertTrue(mainClasses().contain("rice.p2p.glacier.v2.GlacierImpl"), "the glacier module");
    assertFalse(mainClasses().contain(FutureThreadingRulesTest.class), "no test classes");
  }

  /** Breaks every rule, to show that the rules catch it. */
  static final class Violations {
    void asyncOnTheCommonPool(CompletableFuture<String> future) {
      var _ = future.thenApplyAsync(String::length);
    }

    void asyncOnAnExecutor(CompletableFuture<String> future, Executor executor) {
      var _ = future.thenApplyAsync(String::length, executor);
    }

    void realTimeTimeout(CompletableFuture<String> future) {
      var _ = future.orTimeout(1, TimeUnit.SECONDS);
    }

    void commonPool() {
      var _ = ForkJoinPool.commonPool();
    }

    String block(CompletableFuture<String> future) {
      return future.join();
    }
  }

  @Test
  void theRulesCatchViolations() {
    JavaClasses violations = new ClassFileImporter().importClasses(Violations.class);
    assertTrue(NO_ASYNC_WITHOUT_EXECUTOR.evaluate(violations).hasViolation());
    assertTrue(NO_REAL_TIME_TIMEOUTS.evaluate(violations).hasViolation());
    assertTrue(NO_FORK_JOIN_POOL.evaluate(violations).hasViolation());
    assertTrue(BLOCK_ONLY_IN_AWAIT.evaluate(violations).hasViolation());
    // only the executor-less call breaks the first rule
    assertTrue(NO_ASYNC_WITHOUT_EXECUTOR.evaluate(violations).getFailureReport().getDetails().size() == 1);
  }
}
