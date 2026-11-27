package org.mpisws.p2p.transport.util;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Access to the array behind a {@link ByteBuffer}, for the layers that parse messages as byte arrays.
 *
 * <p>Only heap buffers have an accessible array: a direct or read-only buffer has none, and a slice or duplicate
 * of a heap buffer starts at an {@link ByteBuffer#arrayOffset() array offset}. These methods account for the offset
 * and refuse a buffer without an array instead of failing with an unspecific {@code UnsupportedOperationException}
 * or reading the wrong bytes.
 */
public final class ByteBuffers {

  private ByteBuffers() {}

  /**
   * The array behind the buffer.
   *
   * @throws IllegalArgumentException if the buffer has no accessible array
   */
  @SuppressWarnings("ByteBufferBackingArray") // the callers add arrayOffset() through arrayStart() and arrayPosition()
  public static byte[] array(ByteBuffer buffer) {
    if (!buffer.hasArray()) {
      throw new IllegalArgumentException("The buffer has no accessible array (direct or read-only): " + buffer);
    }
    return buffer.array();
  }

  /**
   * The array behind the buffer, for a layer that reads from the wire.
   *
   * @throws IOException if the buffer has no accessible array
   */
  @SuppressWarnings("ByteBufferBackingArray") // the callers add arrayOffset() through arrayStart() and arrayPosition()
  public static byte[] arrayForRead(ByteBuffer buffer) throws IOException {
    if (!buffer.hasArray()) {
      throw new IOException("The buffer has no accessible array (direct or read-only): " + buffer);
    }
    return buffer.array();
  }

  /** The index in {@link #array} of the buffer's first byte, including its array offset. */
  public static int arrayStart(ByteBuffer buffer) {
    return buffer.arrayOffset();
  }

  /** The index in {@link #array} of the buffer's current position. */
  public static int arrayPosition(ByteBuffer buffer) {
    return buffer.arrayOffset() + buffer.position();
  }

  /** Moves the buffer's position to the given index in {@link #array}. */
  public static void setArrayPosition(ByteBuffer buffer, int arrayIndex) {
    buffer.position(arrayIndex - buffer.arrayOffset());
  }

  /** The bytes between the buffer's position and limit, without consuming them. */
  public static byte[] peekRemaining(ByteBuffer buffer) {
    byte[] copy = new byte[buffer.remaining()];
    buffer.duplicate().get(copy);
    return copy;
  }

  /** The up to {@code count} bytes just before the buffer's position, e.g. what a read or write has just moved. */
  public static byte[] peekPreceding(ByteBuffer buffer, long count) {
    ByteBuffer view = buffer.duplicate();
    int end = view.position();
    view.limit(end);
    view.position((int) Math.max(0, end - Math.max(0, count)));
    return peekRemaining(view);
  }
}
