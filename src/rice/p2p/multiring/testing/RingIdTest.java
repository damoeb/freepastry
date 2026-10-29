package rice.p2p.multiring.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.Map;

import org.junit.jupiter.api.Test;

import rice.p2p.multiring.RingId;
import rice.pastry.Id;

/**
 * Regression test for the RingId interning cache: it used to store each RingId as both the key
 * and the value of a WeakHashMap, so the value strongly referenced the key and entries were
 * never collected.  Deliberately does not depend on GC timing.
 */
public class RingIdTest {

  private static final int[] RING = {1, 2, 3, 4, 5};
  private static final int[] ID = {6, 7, 8, 9, 10};

  @Test
  void buildReturnsCanonicalInstance() {
    RingId first = RingId.build(Id.build(RING), Id.build(ID));
    RingId second = RingId.build(Id.build(RING.clone()), Id.build(ID.clone()));

    assertSame(first, second);
    assertEquals(Id.build(RING), first.getRingId());
    assertEquals(Id.build(ID), first.getId());
  }

  @Test
  void cacheDoesNotStronglyReferenceItsKeys() throws Exception {
    RingId ringId = RingId.build(Id.build(RING), Id.build(new int[] {11, 12, 13, 14, 15}));

    Field field = RingId.class.getDeclaredField("RINGID_MAP");
    field.setAccessible(true);
    Map<?, ?> map = (Map<?, ?>) field.get(null);

    Object value;
    synchronized (map) {
      value = map.get(ringId);
    }
    assertNotNull(value, "interned RingId should be in the cache");
    assertFalse(value instanceof RingId, "the cache value must not be a strong reference to the key");
    assertInstanceOf(WeakReference.class, value);
    assertSame(ringId, ((WeakReference<?>) value).get());
  }
}
