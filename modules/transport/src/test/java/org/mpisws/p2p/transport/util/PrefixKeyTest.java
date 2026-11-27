package org.mpisws.p2p.transport.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetAddress;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;

class PrefixKeyTest {
  private static final PrefixKey.Lengths DEFAULT_LENGTHS = new PrefixKey.Lengths(24, 64);

  @Test
  @DisplayName("IPv4 addresses in one /24 share a key, the next /24 has another")
  void ipv4SlashTwentyFour() throws Exception {
    PrefixKey first = DEFAULT_LENGTHS.of(InetAddress.getByName("192.0.2.1"));

    assertEquals(first, DEFAULT_LENGTHS.of(InetAddress.getByName("192.0.2.254")));
    assertEquals(first.hashCode(), DEFAULT_LENGTHS.of(InetAddress.getByName("192.0.2.254")).hashCode());
    assertNotEquals(first, DEFAULT_LENGTHS.of(InetAddress.getByName("192.0.3.1")));
    assertEquals("192.0.2.0/24", first.toString());
  }

  @Test
  @DisplayName("IPv6 addresses in one /64 share a key, the next /64 has another")
  void ipv6SlashSixtyFour() throws Exception {
    PrefixKey first = DEFAULT_LENGTHS.of(InetAddress.getByName("2001:db8:1:2::1"));

    assertEquals(first, DEFAULT_LENGTHS.of(InetAddress.getByName("2001:db8:1:2:ffff:ffff:ffff:ffff")));
    assertNotEquals(first, DEFAULT_LENGTHS.of(InetAddress.getByName("2001:db8:1:3::1")));
  }

  @Test
  @DisplayName("a prefix length that is not a multiple of eight masks inside a byte")
  void prefixInsideAByte() throws Exception {
    PrefixKey.Lengths lengths = new PrefixKey.Lengths(20, 64);

    assertEquals(lengths.of(InetAddress.getByName("10.1.16.1")), lengths.of(InetAddress.getByName("10.1.31.255")));
    assertNotEquals(lengths.of(InetAddress.getByName("10.1.16.1")), lengths.of(InetAddress.getByName("10.1.32.1")));
  }

  @Test
  @DisplayName("the default parameters are /24 and /64, and lengths out of range are refused")
  void defaultsAndRange() {
    Environment env = Environment.directEnvironment();
    try {
      assertEquals(DEFAULT_LENGTHS, PrefixKey.Lengths.fromParameters(env.getParameters()));
    } finally {
      env.destroy();
    }
    assertThrows(IllegalArgumentException.class, () -> new PrefixKey.Lengths(33, 64));
    assertThrows(IllegalArgumentException.class, () -> new PrefixKey.Lengths(24, 129));
    assertThrows(IllegalArgumentException.class, () -> new PrefixKey.Lengths(-1, 64));
  }
}
