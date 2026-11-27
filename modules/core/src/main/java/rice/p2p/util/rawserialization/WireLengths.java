package rice.p2p.util.rawserialization;

import java.io.EOFException;
import java.io.IOException;
import rice.p2p.commonapi.rawserialization.InputBuffer;

/**
 * Validates lengths and counts that were read from the wire before they are used to allocate an array or to bound
 * a loop, so that a peer cannot make us allocate gigabytes by sending a few bytes.
 */
public final class WireLengths {
  private WireLengths() {
  }

  /**
   * Reads an {@code int} count and validates it with {@link #check(InputBuffer, long, int)}.
   *
   * @param minBytesPerElement the least number of bytes each element occupies on the wire (at least 1)
   * @return the count
   * @throws IOException if the count is negative or the buffer cannot hold that many elements
   */
  public static int readInt(InputBuffer buf, int minBytesPerElement) throws IOException {
    return check(buf, buf.readInt(), minBytesPerElement);
  }

  /**
   * Reads a {@code short} count and validates it, see {@link #readInt(InputBuffer, int)}.
   */
  public static int readShort(InputBuffer buf, int minBytesPerElement) throws IOException {
    return check(buf, buf.readShort(), minBytesPerElement);
  }

  /**
   * Reads a {@code byte} count and validates it, see {@link #readInt(InputBuffer, int)}.
   */
  public static int readByte(InputBuffer buf, int minBytesPerElement) throws IOException {
    return check(buf, buf.readByte(), minBytesPerElement);
  }

  /**
   * Reads an {@code int} length and then that many bytes.
   *
   * @throws IOException if the length is negative or exceeds what is left in the buffer, or the buffer ends early
   */
  public static byte[] readBytes(InputBuffer buf) throws IOException {
    return readBytes(buf, readInt(buf, 1));
  }

  /**
   * Reads exactly {@code length} bytes, which the caller has already validated with {@link #check}.
   *
   * @throws IOException if the buffer ends early
   */
  public static byte[] readBytes(InputBuffer buf, int length) throws IOException {
    check(buf, length, 1);
    byte[] result = new byte[length];
    int offset = 0;
    while (offset < length) {
      int n = buf.read(result, offset, length - offset);
      if (n < 0) {
        throw new EOFException("Expected " + length + " bytes, got " + offset);
      }
      offset += n;
    }
    return result;
  }

  /**
   * @param count the count announced by the peer
   * @param minBytesPerElement the least number of bytes each element occupies on the wire (at least 1)
   * @return the count
   * @throws IOException if the count is negative or needs more bytes than {@link InputBuffer#bytesRemaining()}; a
   *         buffer that does not know how much is left can only be checked for negative counts
   */
  public static int check(InputBuffer buf, long count, int minBytesPerElement) throws IOException {
    if (count < 0) {
      throw new IOException("Negative length/count " + count);
    }
    int remaining = buf.bytesRemaining();
    if (remaining >= 0 && count * minBytesPerElement > remaining) {
      throw new IOException("Length/count " + count + " needs at least " + count * minBytesPerElement
          + " bytes, only " + remaining + " left");
    }
    return (int) count;
  }
}
