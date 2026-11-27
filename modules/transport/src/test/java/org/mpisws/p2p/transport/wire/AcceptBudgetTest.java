package org.mpisws.p2p.transport.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.util.PrefixKey;

/** C1 (design 3a): the handshake caps and accept buckets count per network prefix, driven by a manual clock. */
class AcceptBudgetTest {
  private static final long START_MILLIS = 5_000_000;
  private static final int UNLIMITED = 1_000_000;

  @Test
  @DisplayName("ten addresses of one /24 share one handshake cap: 10 addresses with 32 sockets each get 32 in all")
  void handshakeCapCountsPerPrefix() throws Exception {
    AcceptBudget budget = new AcceptBudget(config(32, UNLIMITED, UNLIMITED, UNLIMITED, UNLIMITED, UNLIMITED), START_MILLIS);

    int admitted = 0;
    for (int host = 1; host <= 10; host++) {
      for (int socket = 0; socket < 32; socket++) {
        if (budget.startHandshake(address("192.0.2." + host), START_MILLIS) == null) admitted++;
      }
    }

    assertEquals(32, admitted);
    assertNull(budget.startHandshake(address("192.0.3.1"), START_MILLIS), "another /24 has its own cap");
  }

  @Test
  @DisplayName("a socket that ends its handshake frees its place in the cap of its prefix and in the global cap")
  void handshakeEndFreesPlace() throws Exception {
    AcceptBudget budget = new AcceptBudget(config(1, 2, UNLIMITED, UNLIMITED, UNLIMITED, UNLIMITED), START_MILLIS);
    assertNull(budget.startHandshake(address("192.0.2.1"), START_MILLIS));
    assertNotNull(budget.startHandshake(address("192.0.2.2"), START_MILLIS), "the prefix is at its cap");
    assertNull(budget.startHandshake(address("198.51.100.1"), START_MILLIS));
    assertNotNull(budget.startHandshake(address("203.0.113.1"), START_MILLIS), "the node is at its global cap");

    budget.handshakeEnded(address("192.0.2.1"));

    assertEquals(1, budget.getHandshakingSocketCount());
    assertNull(budget.startHandshake(address("192.0.2.2"), START_MILLIS));
  }

  @Test
  @DisplayName("a prefix that used up its accept burst is refused until its bucket refills, others are not")
  void acceptBucketPerPrefix() throws Exception {
    AcceptBudget budget = new AcceptBudget(config(UNLIMITED, UNLIMITED, 10, 20, UNLIMITED, UNLIMITED), START_MILLIS);
    for (int i = 0; i < 20; i++) {
      assertNull(budget.startHandshake(address("192.0.2.1"), START_MILLIS), "socket " + i + " of the burst");
    }

    assertNotNull(budget.startHandshake(address("192.0.2.9"), START_MILLIS), "the burst of the /24 is used up");
    assertNull(budget.startHandshake(address("198.51.100.1"), START_MILLIS), "another prefix has its own bucket");
    assertNull(budget.startHandshake(address("192.0.2.9"), START_MILLIS + 100), "one token after 100 ms at 10/s");
  }

  @Test
  @DisplayName("the global accept bucket bounds all prefixes together, and a refused prefix does not drain it")
  void globalAcceptBucket() throws Exception {
    AcceptBudget budget = new AcceptBudget(config(UNLIMITED, UNLIMITED, 1, 1, 1, 3), START_MILLIS);
    assertNull(budget.startHandshake(address("192.0.2.1"), START_MILLIS));
    for (int i = 0; i < 10; i++) {
      assertNotNull(budget.startHandshake(address("192.0.2.1"), START_MILLIS), "refused by the bucket of its prefix");
    }
    assertNull(budget.startHandshake(address("198.51.100.1"), START_MILLIS));
    assertNull(budget.startHandshake(address("203.0.113.1"), START_MILLIS));

    assertNotNull(budget.startHandshake(address("233.252.0.1"), START_MILLIS), "the global burst of 3 is used up");
  }

  private static InetAddress address(String literal) throws UnknownHostException {
    return InetAddress.getByName(literal);
  }

  private static WireConfig config(int maxPerPrefix, int maxInAll, double ratePerPrefix, double burstPerPrefix, double rate, double burst) {
    return new WireConfig(false, 300, 32768, false, 131072, 65536, 10_000, maxPerPrefix, maxInAll,
        ratePerPrefix, burstPerPrefix, rate, burst, 64, 200, 400, new PrefixKey.Lengths(24, 64), 10_000);
  }
}
