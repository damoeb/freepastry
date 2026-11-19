package rice.p2p.glacier.v2;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * A peer sends its Bloom filter in a sync request, and the node probes it with the keys of its own fragments: a hash
 * parameter of zero divides by zero, and one larger than the bit field indexes past it.
 */
class BloomFilterWireTest {
  private static final int BITFIELD_BYTES = 4;
  private static final int BITS = BITFIELD_BYTES * 8;
  private static final byte[] KEY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};

  private static SimpleInputBuffer filter(int hashParam) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeInt(1);
    out.writeInt(hashParam);
    out.writeInt(BITFIELD_BYTES);
    out.write(new byte[BITFIELD_BYTES], 0, BITFIELD_BYTES);
    return new SimpleInputBuffer(out.getBytes());
  }

  @ParameterizedTest(name = "hash parameter {0}")
  @ValueSource(ints = {0, -1, -7, BITS + 1, Integer.MAX_VALUE, Integer.MIN_VALUE})
  @DisplayName("a hash parameter outside 1 to the number of bits is rejected with an IOException")
  void rejectsAHashParameterOutsideTheBitField(int hashParam) throws IOException {
    SimpleInputBuffer buf = filter(hashParam);

    assertThrows(IOException.class, () -> new FragmentBloomFilter(buf).contains(KEY));
  }

  @ParameterizedTest(name = "hash parameter {0}")
  @ValueSource(ints = {1, 7, BITS})
  @DisplayName("a hash parameter from 1 to the number of bits is read and probed")
  void readsAHashParameterInsideTheBitField(int hashParam) throws IOException {
    FragmentBloomFilter read = new FragmentBloomFilter(filter(hashParam));
    read.add(KEY);

    assertTrue(read.contains(KEY));
  }
}
