package rice.pastry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import rice.p2p.util.IdBloomFilter;
import rice.pastry.testing.WireRoundTrip;

/**
 * An {@link IdBloomFilter} through the bytes it is sent in (Past and Glacier send it to ask for the ids a node is
 * missing): the copy must find every id of the set and answer every probe as the original does.
 */
class IdBloomFilterWireRoundTripTest {
  private static IdSet idSet(int count, long seed) {
    Random random = new Random(seed);
    IdSet set = new IdSet();
    for (int i = 0; i < count; i++) {
      set.addId(Id.makeRandomId(random));
    }
    return set;
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 63, 64, 65, 200, 1000})
  @DisplayName("A filter over a set of any size, empty to larger than its minimum size, answers as before")
  void idBloomFilter(int count) throws IOException {
    IdSet set = idSet(count, count);
    IdBloomFilter filter = new IdBloomFilter(set);

    IdBloomFilter copy = WireRoundTrip.roundTrip(filter, IdBloomFilter::serialize, IdBloomFilter::new);

    java.util.Iterator<rice.p2p.commonapi.Id> it = set.getIterator();
    while (it.hasNext()) {
      assertTrue(copy.check((Id) it.next()), "an id of the set is found");
    }
    Random probes = new Random(~count);
    for (int i = 0; i < 500; i++) {
      Id probe = Id.makeRandomId(probes);
      assertEquals(filter.check(probe), copy.check(probe));
    }
  }
}
