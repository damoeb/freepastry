package rice.p2p.util.testing;

import org.junit.jupiter.api.Test;

import static rice.p2p.util.testing.MainProgram.assertContains;
import static rice.p2p.util.testing.MainProgram.assertNoneOf;
import static rice.p2p.util.testing.MainProgram.run;

/**
 * Runs the legacy main()-based checks of rice.p2p.util, which report failures by printing a marker (or throwing).
 */
public class UtilUnitProgramsTest {

  @Test
  public void bloomFilterHasNoFalseNegatives() throws Exception {
    // 3 hash functions, 20-byte elements, 1000 bits, 100 elements
    String output = run(BloomFilterUnit::main, "3", "20", "1000", "100");
    assertNoneOf(output, "FAILURE");
    assertContains(output, "FALSE POSITIVE RATE");
  }

  @Test
  public void idBloomFilter() throws Exception {
    assertNoneOf(run(IdBloomFilterUnit::main), "FAILURE");
  }

  @Test
  public void encryptedStream() throws Exception {
    String output = run(EncryptedStreamUnit::main);
    assertNoneOf(output, "FAILED");
    assertContains(output, "[ PASSED ]");
  }

  @Test
  public void mathUtils() throws Exception {
    String output = run(MathUtilsUnit::main);
    assertNoneOf(output, "FAILED");
    assertContains(output, "[ PASSED ]");
  }

  @Test
  public void securityUtils() throws Exception {
    String output = run(SecurityUtilsUnit::main);
    assertNoneOf(output, "FAILED");
    assertContains(output, "[ PASSED ]");
  }

  @Test
  public void stringCache() throws Exception {
    String output = run(StringCacheUnit::main);
    assertNoneOf(output, "FAILED");
    assertContains(output, "[ PASSED ]");
  }

  @Test
  public void xmlParser() throws Exception {
    String output = run(XMLParserUnit::main);
    assertNoneOf(output, "FAILED");
    assertContains(output, "[ PASSED ]");
  }

  @Test
  public void redBlackMap() throws Exception {
    // throws a RuntimeException on the first wrong (sub-)map; 8 entries, 8 random sub-maps per level, 3 levels
    String output = run(RedBlackMapUnit::main, "8");
    assertContains(output, "REMOVING");
  }

  @Test
  public void sortedLinkedList() throws Exception {
    // throws a RuntimeException if the list is out of order
    run(TestSortedLinkedList::main);
  }
}
