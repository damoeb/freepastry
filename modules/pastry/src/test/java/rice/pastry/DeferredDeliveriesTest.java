package rice.pastry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DeferredDeliveriesTest {

  static final Id BOB = Id.build("1234567890ABCDEF1234567890ABCDEF12345678");
  static final Id CAROL = Id.build("00000000000000000000000000000000000000CC");

  final List<Runnable> timers = new ArrayList<>();
  final List<Long> delays = new ArrayList<>();
  final List<String> delivered = new ArrayList<>();
  final DeferredDeliveries target = new DeferredDeliveries((task, delay) -> {
    timers.add(task);
    delays.add(delay);
  });

  Runnable message(String name) {
    return () -> delivered.add(name);
  }

  @Test
  @DisplayName("messages from an unverified peer are delivered in order after a matching certificate")
  void deliversInOrderWhenAuthenticated() {
    assertTrue(target.defer(BOB, message("1")));
    assertTrue(target.defer(BOB, message("2")));
    assertTrue(delivered.isEmpty());

    target.resolve(BOB, true);

    assertEquals(List.of("1", "2"), delivered);
    assertEquals(0, target.size());
  }

  @Test
  @DisplayName("messages from an unverified peer are dropped after a mismatching certificate")
  void dropsWhenVerificationFails() {
    target.defer(BOB, message("1"));

    target.resolve(BOB, false);
    target.resolve(BOB, true);

    assertTrue(delivered.isEmpty());
    assertEquals(0, target.size());
  }

  @Test
  @DisplayName("messages are dropped when the peer is not verified within the timeout")
  void dropsAfterTimeout() {
    target.defer(BOB, message("1"));
    target.defer(BOB, message("2"));

    assertEquals(List.of(DeferredDeliveries.VERIFY_TIMEOUT_MILLIS), delays, "one timer per peer");
    timers.get(0).run();
    target.resolve(BOB, true);

    assertTrue(delivered.isEmpty());
    assertEquals(0, target.size());
  }

  @Test
  @DisplayName("a timer of an earlier wait does not drop the messages of a later one")
  void staleTimerIsIgnored() {
    target.defer(BOB, message("1"));
    target.resolve(BOB, true);
    target.defer(BOB, message("2"));

    timers.get(0).run();
    assertEquals(1, target.size());

    target.resolve(BOB, true);
    assertEquals(List.of("1", "2"), delivered);
  }

  @Test
  @DisplayName("a peer may have at most MAX_PER_PEER messages waiting")
  void boundsMessagesPerPeer() {
    for (int i = 0; i < DeferredDeliveries.MAX_PER_PEER; i++) {
      assertTrue(target.defer(BOB, message("m" + i)));
    }

    assertFalse(target.defer(BOB, message("over")));
    assertTrue(target.defer(CAROL, message("other")), "another peer is not affected");
    assertEquals(DeferredDeliveries.MAX_PER_PEER + 1, target.size());
  }

  @Test
  @DisplayName("at most MAX_TOTAL messages wait for all peers together, and room frees up on resolve")
  void boundsMessagesInTotal() {
    int peers = DeferredDeliveries.MAX_TOTAL / DeferredDeliveries.MAX_PER_PEER;
    List<Id> ids = new ArrayList<>();
    for (int p = 0; p < peers; p++) {
      Id id = Id.build(String.format("%040X", p + 1));
      ids.add(id);
      for (int i = 0; i < DeferredDeliveries.MAX_PER_PEER; i++) {
        assertTrue(target.defer(id, message("x")));
      }
    }
    assertEquals(DeferredDeliveries.MAX_TOTAL, target.size());

    assertFalse(target.defer(CAROL, message("flood")));

    target.resolve(ids.get(0), false);
    assertTrue(target.defer(CAROL, message("fits now")));
  }
}
