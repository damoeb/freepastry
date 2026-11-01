package rice.p2p.util.ids;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdSet;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.FragmentKeyFactory;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.VersionKeyFactory;
import rice.p2p.multiring.MultiringIdFactory;
import rice.p2p.multiring.RingId;
import rice.p2p.past.gc.GCId;
import rice.p2p.past.gc.GCIdFactory;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the content hash, iteration order and string form of every IdSet implementation. IdSet.hash() drives replica
 * synchronisation and the iteration order drives which keys are fetched first, so refactoring the IdSet classes must
 * leave all of these unchanged. The expected values were recorded before the Id classes were unified.
 */
class IdSetCharacterizationTest {

  private static final String RING_HEX = "4242424242424242424242424242424242424242";

  /** Insertion order: deliberately unsorted. */
  private static final String[] HEX = {
    "F00000000000000000000000000000000000000D",
    "123456789ABCDEF0123456789ABCDEF012345678",
    "8000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000001",
    "80000000000000000000000000000000000000FF",
    "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
    "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
    "8ABCDEF000000000000000000000000000000000"
  };

  /** Iteration order. */
  private static final String[] SORTED_HEX = {
    "0000000000000000000000000000000000000001",
    "123456789ABCDEF0123456789ABCDEF012345678",
    "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
    "8000000000000000000000000000000000000000",
    "80000000000000000000000000000000000000FF",
    "8ABCDEF000000000000000000000000000000000",
    "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
    "F00000000000000000000000000000000000000D"
  };

  /** The members of SORTED_HEX that start with "8". */
  private static final int PREFIX_FROM = 3;
  private static final int PREFIX_TO = 6;

  private static final String FULL_HASH = "A300E4793834D8919692DD323250617630BB4C9E02CC497464AFA19431CE84D3";
  private static final String PREFIX_8_HASH = "178EA609B42BD8E10BDA1F35068321B52AE11FD6F0BD6C3F5171883F63D214F2";
  private static final String EMPTY_HASH = "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855";

  private static final String PASTRY_STRING =
      "[ IdSet: <0x000000..>,<0x123456..>,<0x7FFFFF..>,<0x800000..>,<0x800000..>,<0x8ABCDE..>,<0xDEADBE..>,"
          + "<0xF00000..>, ]";

  private final PastryIdFactory pastryFactory = new PastryIdFactory(null);
  private final Id ring = rice.pastry.Id.build(RING_HEX);
  private final MultiringIdFactory multiringFactory = new MultiringIdFactory(ring, pastryFactory);

  private static List<Id> pastryIds() {
    List<Id> ids = new ArrayList<>();
    for (String hex : HEX) {
      ids.add(rice.pastry.Id.build(hex));
    }
    return ids;
  }

  private static IdSet fill(IdSet set, List<? extends Id> ids) {
    for (Id id : ids) {
      set.addId(id);
    }
    return set;
  }

  private static String order(IdSet set) {
    List<String> result = new ArrayList<>();
    Iterator<Id> it = set.getIterator();
    while (it.hasNext()) {
      result.add(it.next().toStringFull());
    }
    return String.join(",", result);
  }

  /** The sorted ids from index from (inclusive) to to (exclusive), each followed by its suffixes. */
  private static String expected(String prefix, int from, int to, String... suffixes) {
    List<String> result = new ArrayList<>();
    for (int i = from; i < to; i++) {
      @SuppressWarnings("StringSplitter") // the test data has no trailing blanks
      String[] split = suffixes[i].split(" ");
      for (String suffix : split) {
        result.add(prefix + SORTED_HEX[i] + suffix);
      }
    }
    return String.join(",", result);
  }

  private static String hex(byte[] hash) {
    return HexFormat.of().withUpperCase().formatHex(hash);
  }

  @Test
  void pastryIdSetKeepsHashOrderAndString() {
    IdSet set = fill(pastryFactory.buildIdSet(), pastryIds());
    String[] none = {"", "", "", "", "", "", "", ""};

    assertEquals(expected("", 0, 8, none), order(set));
    assertEquals(FULL_HASH, hex(set.hash()));
    assertEquals(EMPTY_HASH, hex(pastryFactory.buildIdSet().hash()));
    assertEquals(PASTRY_STRING, set.toString());

    IdSet prefix = set.subSet(pastryFactory.buildIdRangeFromPrefix("8"));
    assertEquals(expected("", PREFIX_FROM, PREFIX_TO, none), order(prefix));
    assertEquals(PREFIX_8_HASH, hex(prefix.hash()));
  }

  @Test
  void pastryIdSetRecomputesHashAfterMutation() {
    IdSet set = fill(pastryFactory.buildIdSet(), pastryIds());
    set.hash();
    set.removeId(rice.pastry.Id.build(HEX[0]));
    String withoutFirst = hex(set.hash());
    set.addId(rice.pastry.Id.build(HEX[0]));

    assertEquals(FULL_HASH, hex(set.hash()));
    assertEquals(false, FULL_HASH.equals(withoutFirst));
  }

  @Test
  void multiringIdSetKeepsHashOrderAndString() {
    List<Id> ringIds = new ArrayList<>();
    for (Id id : pastryIds()) {
      ringIds.add(RingId.build(ring, id));
    }
    IdSet set = fill(multiringFactory.buildIdSet(), ringIds);
    String[] close = {")", ")", ")", ")", ")", ")", ")", ")"};

    assertEquals(expected("(" + RING_HEX + ", ", 0, 8, close), order(set));
    assertEquals(FULL_HASH, hex(set.hash()));
    assertEquals("{RingId <0x424242..> " + PASTRY_STRING + "}", set.toString());
    assertEquals(PREFIX_8_HASH, hex(set.subSet(multiringFactory.buildIdRangeFromPrefix("8")).hash()));
  }

  @Test
  void versionKeySetKeepsOrderAndString() {
    IdFactory factory = new VersionKeyFactory(multiringFactory);
    List<Id> keys = new ArrayList<>();
    long version = 7;
    for (Id id : pastryIds()) {
      keys.add(new VersionKey(RingId.build(ring, id), version));
      keys.add(new VersionKey(RingId.build(ring, id), version - 5));
      version = version * 31 + 3;
    }
    IdSet set = fill(factory.buildIdSet(), keys);
    String[] versions = {
      ")v211511 )v211516",
      ")v215 )v220",
      ")v6301276130 )v6301276135",
      ")v6818 )v6823",
      ")v6556994 )v6556999",
      ")v195339560183 )v195339560188",
      ")v203266967 )v203266972",
      ")v2 )v7"
    };

    assertEquals(expected("(" + RING_HEX + ", ", 0, 8, versions), order(set));
    assertTrue(set.toString().startsWith("[ IdSet: (") && set.toString().contains(")v211511,"));
    assertEquals(
        expected("(" + RING_HEX + ", ", PREFIX_FROM, PREFIX_TO, versions),
        order(set.subSet(factory.buildIdRangeFromPrefix("8"))));
  }

  @Test
  void fragmentKeySetKeepsOrderAndString() {
    IdFactory factory = new FragmentKeyFactory(multiringFactory);
    List<Id> keys = new ArrayList<>();
    int fragment = 3;
    for (Id id : pastryIds()) {
      VersionKey versionKey = new VersionKey(RingId.build(ring, id), fragment * 11L);
      keys.add(new FragmentKey(versionKey, fragment));
      keys.add(new FragmentKey(versionKey, fragment + 40));
      fragment = (fragment * 7) % 19;
    }
    IdSet set = fill(factory.buildIdSet(), keys);
    String[] fragments = {
      ")v33#3 )v33#43",
      ")v22#2 )v22#42",
      ")v33#3 )v33#43",
      ")v154#14 )v154#54",
      ")v22#2 )v22#42",
      ")v22#2 )v22#42",
      ")v154#14 )v154#54",
      ")v33#3 )v33#43"
    };

    assertEquals(expected("(" + RING_HEX + ", ", 0, 8, fragments), order(set));
    String head = "[ IdSet: (<0x424242..>, <0x000000..>)v33#3,(<0x424242..>, <0x000000..>)v33#43,";
    String tail = ",(<0x424242..>, <0xF00000..>)v33#3,(<0x424242..>, <0xF00000..>)v33#43, ]";
    assertEquals(head, set.toString().substring(0, head.length()));
    assertEquals(tail, set.toString().substring(set.toString().length() - tail.length()));
    assertEquals(
        expected("(" + RING_HEX + ", ", PREFIX_FROM, PREFIX_TO, fragments),
        order(set.subSet(factory.buildIdRangeFromPrefix("8"))));
  }

  @Test
  void gcIdSetKeepsOrderAndString() {
    IdFactory factory = new GCIdFactory(pastryFactory);
    List<Id> keys = new ArrayList<>();
    long expiration = 1000;
    for (Id id : pastryIds()) {
      keys.add(new GCId(id, expiration));
      expiration += 977;
    }
    IdSet set = fill(factory.buildIdSet(), keys);
    String[] expirations = {"-3931", "-1977", "-6862", "-2954", "-4908", "-7839", "-5885", "-1000"};

    assertEquals(expected("", 0, 8, expirations), order(set));
    assertEquals("{GCIdSet of size 8}", set.toString());
  }
}
