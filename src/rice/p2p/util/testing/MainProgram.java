package rice.p2p.util.testing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Runs a legacy main()-based check program inside a Jupiter test and asserts on what it prints. Many of the old
 * "*Unit" programs report failures only by printing a marker such as "[ FAILED ]" and still return normally.
 */
public final class MainProgram {

  /**
   * The main() method of a program.
   */
  public interface Main {
    void main(String[] args) throws Exception;
  }

  private MainProgram() {
  }

  /**
   * Runs the program with System.out and System.err captured (the tests run sequentially), and returns the output.
   * Whatever the program throws propagates.
   */
  public static String run(Main main, String... args) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    PrintStream capture = new PrintStream(buffer, true, StandardCharsets.UTF_8);
    PrintStream out = System.out;
    PrintStream err = System.err;
    System.setOut(capture);
    System.setErr(capture);
    try {
      main.main(args);
    } finally {
      System.setOut(out);
      System.setErr(err);
      capture.flush();
    }
    return buffer.toString(StandardCharsets.UTF_8);
  }

  /**
   * Asserts that the output contains none of the failure markers.
   */
  public static void assertNoneOf(String output, String... failureMarkers) {
    for (String marker : failureMarkers)
      assertFalse(output.contains(marker), () -> "Output contains \"" + marker + "\":\n" + output);
  }

  /**
   * Asserts that the output contains the marker, i.e. the program ran to that point.
   */
  public static void assertContains(String output, String marker) {
    assertTrue(output.contains(marker), () -> "Output lacks \"" + marker + "\":\n" + output);
  }
}
