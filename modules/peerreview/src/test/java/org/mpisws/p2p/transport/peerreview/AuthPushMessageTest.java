package org.mpisws.p2p.transport.peerreview;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.peerreview.message.AuthPushMessage;
import org.mpisws.p2p.transport.util.Serializer;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

class AuthPushMessageTest {
  private static final int HASH_SIZE = 20;
  private static final int SIGNATURE_SIZE = 64;

  record TestId(long value) implements RawSerializable {
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeLong(value);
    }
  }

  private static final Serializer<TestId> ID_SERIALIZER = new Serializer<>() {
    @Override
    public void serialize(TestId testId, OutputBuffer buf) throws IOException {
      testId.serialize(buf);
    }

    @Override
    public TestId deserialize(InputBuffer buf) throws IOException {
      return new TestId(buf.readLong());
    }
  };

  private static final AuthenticatorSerializer AUTHENTICATORS = new AuthenticatorSerializer(HASH_SIZE,
      SIGNATURE_SIZE);

  private static AuthPushMessage<TestId> build(SimpleOutputBuffer out) throws IOException {
    return AuthPushMessage.build(new SimpleInputBuffer(out.getBytes()), ID_SERIALIZER, AUTHENTICATORS);
  }

  @Test
  @DisplayName("rejects an identifier count the message cannot hold")
  void rejectsOversizedIdentifierCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeShort(Short.MAX_VALUE);

    assertThrows(IOException.class, () -> build(out));
  }

  @Test
  @DisplayName("rejects a negative identifier count")
  void rejectsNegativeIdentifierCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeShort((short) -1);

    assertThrows(IOException.class, () -> build(out));
  }

  @Test
  @DisplayName("rejects an authenticator count the message cannot hold")
  void rejectsOversizedAuthenticatorCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeShort((short) 1);
    out.writeLong(1L);
    out.writeShort(Short.MAX_VALUE);

    assertThrows(IOException.class, () -> build(out));
  }

  @Test
  @DisplayName("rejects counts above the maximum even when the bytes are present")
  void rejectsCountAboveMaximum() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeShort((short) 1);
    out.writeLong(1L);
    out.writeShort((short) (AuthPushMessage.MAX_AUTHENTICATORS_PER_ID + 1));
    out.write(new byte[(AuthPushMessage.MAX_AUTHENTICATORS_PER_ID + 1) * (8 + HASH_SIZE + SIGNATURE_SIZE)], 0,
        (AuthPushMessage.MAX_AUTHENTICATORS_PER_ID + 1) * (8 + HASH_SIZE + SIGNATURE_SIZE));

    assertThrows(IOException.class, () -> build(out));
  }

  @Test
  @DisplayName("accepts an empty push")
  void acceptsEmptyPush() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeShort((short) 0);

    assertEquals(0, build(out).authenticators().size());
  }
}
