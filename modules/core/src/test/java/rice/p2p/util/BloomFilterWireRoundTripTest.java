package rice.p2p.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Writes a bloom filter to bytes, reads it back from the bytes, writes the copy and reads it again, as a node that
 * receives a filter and passes it on does; the copy must answer every question as the original does.
 */
class BloomFilterWireRoundTripTest {
  private static byte[] bytes(BloomFilter filter) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    filter.serialize(out);
    return out.getBytes();
  }

  private static BloomFilter read(byte[] bytes) throws IOException {
    SimpleInputBuffer in = new SimpleInputBuffer(bytes);
    BloomFilter filter = new BloomFilter(in);
    assertEquals(0, in.bytesRemaining(), "bytes left after reading the filter");
    return filter;
  }

  private static BloomFilter roundTrip(BloomFilter filter) throws IOException {
    byte[] first = bytes(filter);
    BloomFilter copy = read(first);
    byte[] second = bytes(copy);
    assertArrayEquals(first, second, "the copy serializes to the same bytes");
    return read(second);
  }

  private static List<byte[]> elements(int count, long seed) {
    Random random = new Random(seed);
    List<byte[]> ret = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      byte[] element = new byte[1 + random.nextInt(20)];
      random.nextBytes(element);
      ret.add(element);
    }
    return ret;
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 255, 256, 1000, 4096})
  @DisplayName("A filter answers as before after a round trip, for lengths around every byte and word boundary")
  void answersAsBefore(int length) throws IOException {
    for (int hashes : new int[] {1, 2, 5}) {
      BloomFilter filter = new BloomFilter(hashes, length);
      List<byte[]> added = elements(length / 4 + 1, length);
      for (byte[] element : added) {
        filter.add(element);
      }

      BloomFilter copy = roundTrip(filter);

      for (byte[] element : added) {
        assertTrue(copy.check(element), "an added element is found");
      }
      for (byte[] probe : elements(500, ~length)) {
        assertEquals(filter.check(probe), copy.check(probe));
      }
    }
  }

  @Test
  @DisplayName("An empty filter, one without hash functions, and one with every bit set round-trip")
  void edgeFilters() throws IOException {
    BloomFilter empty = new BloomFilter(2, 64);
    BloomFilter noHashes = new BloomFilter(0, 64);
    BloomFilter full = new BloomFilter(1, 64);
    for (byte[] element : elements(2000, 1)) {
      full.add(element);
    }

    for (BloomFilter filter : new BloomFilter[] {empty, noHashes, full}) {
      BloomFilter copy = roundTrip(filter);

      for (byte[] probe : elements(200, 2)) {
        assertEquals(filter.check(probe), copy.check(probe));
      }
    }
  }
}
