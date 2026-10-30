package org.mpisws.p2p.transport.peerreview.history.hasher;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import rice.p2p.util.HashUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SHA256HashProviderTest {

  @Test
  void hashesAre32ByteSha256() {
    SHA256HashProvider testObj = new SHA256HashProvider();
    byte[] abc = "abc".getBytes(StandardCharsets.US_ASCII);

    assertEquals(32, testObj.getHashSizeBytes());
    assertEquals(32, testObj.getEmptyHash().length);
    assertArrayEquals(HashUtils.hash(abc), testObj.hash(ByteBuffer.wrap("a".getBytes(StandardCharsets.US_ASCII)), ByteBuffer.wrap("bc".getBytes(StandardCharsets.US_ASCII))));
  }
}
