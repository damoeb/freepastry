package org.mpisws.p2p.transport.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import org.jspecify.annotations.Nullable;
import rice.environment.params.Parameters;

/**
 * The network prefix of an IP address, as a key for budgets that many addresses of one network share: by default an
 * IPv4 /24 or an IPv6 /64 ({@code transport_prefix_bits_v4}, {@code transport_prefix_bits_v6}), so that an attacker
 * with a block of addresses gets one allowance, not one per address.
 */
public final class PrefixKey {
  /** the parameter that sets the prefix length of IPv4 addresses, in bits */
  public static final String PREFIX_BITS_V4_PARAM = "transport_prefix_bits_v4";
  /** the parameter that sets the prefix length of IPv6 addresses, in bits */
  public static final String PREFIX_BITS_V6_PARAM = "transport_prefix_bits_v6";

  private static final int IPV4_BITS = 32;
  private static final int IPV6_BITS = 128;
  private static final int BITS_PER_BYTE = 8;

  private final byte[] maskedAddress;
  private final int prefixBits;

  private PrefixKey(byte[] maskedAddress, int prefixBits) {
    this.maskedAddress = maskedAddress;
    this.prefixBits = prefixBits;
  }

  /** How long the prefixes of IPv4 and IPv6 addresses are. */
  public record Lengths(int ipv4PrefixBits, int ipv6PrefixBits) {
    /** @throws IllegalArgumentException if a length is outside 0..32 (IPv4) or 0..128 (IPv6) */
    public Lengths {
      if (ipv4PrefixBits < 0 || ipv4PrefixBits > IPV4_BITS) {
        throw new IllegalArgumentException(PREFIX_BITS_V4_PARAM + " must be in 0.." + IPV4_BITS + ": " + ipv4PrefixBits);
      }
      if (ipv6PrefixBits < 0 || ipv6PrefixBits > IPV6_BITS) {
        throw new IllegalArgumentException(PREFIX_BITS_V6_PARAM + " must be in 0.." + IPV6_BITS + ": " + ipv6PrefixBits);
      }
    }

    /** @throws IllegalArgumentException if a parameter is out of range */
    public static Lengths fromParameters(Parameters parameters) {
      return new Lengths(parameters.getInt(PREFIX_BITS_V4_PARAM), parameters.getInt(PREFIX_BITS_V6_PARAM));
    }

    /** The prefix of this address. */
    public PrefixKey of(InetAddress address) {
      return PrefixKey.of(address, this);
    }
  }

  /** The prefix of the address: its first {@code ipv4PrefixBits} or {@code ipv6PrefixBits} bits, the rest zero. */
  public static PrefixKey of(InetAddress address, Lengths lengths) {
    byte[] bytes = address.getAddress();
    int prefixBits = bytes.length * BITS_PER_BYTE == IPV4_BITS ? lengths.ipv4PrefixBits() : lengths.ipv6PrefixBits();
    for (int bit = prefixBits; bit < bytes.length * BITS_PER_BYTE; bit++) {
      bytes[bit / BITS_PER_BYTE] &= (byte) ~(0x80 >>> (bit % BITS_PER_BYTE));
    }
    return new PrefixKey(bytes, prefixBits);
  }

  @Override
  public boolean equals(@Nullable Object other) {
    return other instanceof PrefixKey that && prefixBits == that.prefixBits && Arrays.equals(maskedAddress, that.maskedAddress);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(maskedAddress) + prefixBits;
  }

  /** For example {@code 192.0.2.0/24}. */
  @Override
  public String toString() {
    try {
      return InetAddress.getByAddress(maskedAddress).getHostAddress() + "/" + prefixBits;
    } catch (UnknownHostException impossible) {
      // the array has the length of an IPv4 or IPv6 address
      return Arrays.toString(maskedAddress) + "/" + prefixBits;
    }
  }
}
