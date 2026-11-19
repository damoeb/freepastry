package rice.p2p.commonapi.testing;

import java.io.IOException;

import rice.environment.Environment;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs a {@link CommonAPITest} regression program inside a Jupiter test, on the direct (simulated) network of the
 * default parameters, and fails the test if a step failed instead of exiting the JVM.
 */
public final class RegressionRun {

  /**
   * Builds the test program on the environment.
   */
  public interface Factory<T extends CommonAPITest> {
    T create(Environment env) throws IOException;
  }

  /**
   * Adjusts the parameters or the program before it starts.
   */
  public interface Setup<T extends CommonAPITest> {
    void prepare(Environment env, T test) throws IOException;
  }

  private RegressionRun() {
  }

  public static <T extends CommonAPITest> void run(Factory<T> factory) throws IOException {
    run(factory, (env, test) -> {});
  }

  public static <T extends CommonAPITest> void run(Factory<T> factory, Setup<? super T> setup) throws IOException {
    Environment env = CommonAPITest.parseArgs(new String[] {"-protocol", CommonAPITest.PROTOCOL_DIRECT});
    env.getParameters().setBoolean("commonapi_testing_exit_on_failure", false);
    try {
      T test = factory.create(env);
      setup.prepare(env, test);
      test.start();
      assertTrue(test.getFailures().isEmpty(), () -> "Failed steps: " + test.getFailures());
    } finally {
      env.destroy();
    }
  }
}
