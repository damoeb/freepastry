package org.mpisws.p2p.transport.peerreview;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtAck;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSend;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSign;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.util.Serializer;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/** Fixed-size byte fields must be read completely: a message that ends early is rejected, not zero-padded. */
class ShortReadTest {
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
    public void serialize(TestId i, OutputBuffer buf) throws IOException {
      i.serialize(buf);
    }

    @Override
    public TestId deserialize(InputBuffer buf) throws IOException {
      return new TestId(buf.readLong());
    }
  };

  private static byte[] filled(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (i + 1);
    }
    return bytes;
  }

  private static SimpleInputBuffer authenticator(int hashBytes) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(7L);
    out.write(filled(hashBytes), 0, hashBytes);
    return new SimpleInputBuffer(out.getBytes());
  }

  @Test
  @DisplayName("Authenticator reads the whole hash and signature")
  void authenticatorReadsAllBytes() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(7L);
    out.write(filled(HASH_SIZE), 0, HASH_SIZE);
    out.write(filled(SIGNATURE_SIZE), 0, SIGNATURE_SIZE);

    Authenticator actual = new Authenticator(new SimpleInputBuffer(out.getBytes()), HASH_SIZE, SIGNATURE_SIZE);

    assertArrayEquals(filled(HASH_SIZE), actual.getHash());
    assertArrayEquals(filled(SIGNATURE_SIZE), actual.getSignature());
  }

  @Test
  @DisplayName("Authenticator rejects a hash cut short")
  void authenticatorRejectsShortHash() throws IOException {
    SimpleInputBuffer buf = authenticator(HASH_SIZE - 5);

    assertThrows(IOException.class, () -> new Authenticator(buf, HASH_SIZE, SIGNATURE_SIZE));
  }

  @Test
  @DisplayName("Authenticator rejects a signature cut short")
  void authenticatorRejectsShortSignature() throws IOException {
    SimpleInputBuffer buf = authenticator(HASH_SIZE + 5);

    assertThrows(IOException.class, () -> new Authenticator(buf, HASH_SIZE, SIGNATURE_SIZE));
  }

  @Test
  @DisplayName("AckMessage rejects a hash cut short")
  void ackRejectsShortHash() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(1L);
    out.writeLong(2L);
    out.writeLong(3L);
    out.write(filled(5), 0, 5);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());

    assertThrows(IOException.class, () -> AckMessage.build(buf, ID_SERIALIZER, HASH_SIZE, SIGNATURE_SIZE));
  }

  @Test
  @DisplayName("UserDataMessage rejects a hash cut short")
  void userDataRejectsShortHash() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(1L);
    out.writeLong(2L);
    out.write(filled(5), 0, 5);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());

    assertThrows(IOException.class, () -> UserDataMessage.build(buf, ID_SERIALIZER, HASH_SIZE, SIGNATURE_SIZE));
  }

  @Test
  @DisplayName("EvtSign rejects a signature cut short")
  void evtSignRejectsShortSignature() throws IOException {
    SimpleInputBuffer buf = new SimpleInputBuffer(filled(HASH_SIZE + 5));

    assertThrows(IOException.class, () -> new EvtSign(buf, HASH_SIZE, SIGNATURE_SIZE));
  }

  @Test
  @DisplayName("EvtAck rejects a hash cut short")
  void evtAckRejectsShortHash() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(1L);
    out.writeLong(2L);
    out.writeLong(3L);
    out.write(filled(5), 0, 5);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());

    assertThrows(IOException.class, () -> new EvtAck<>(buf, ID_SERIALIZER, HASH_SIZE, SIGNATURE_SIZE));
  }

  @Test
  @DisplayName("EvtSend rejects a hashed entry shorter than the hash")
  void evtSendRejectsPayloadShorterThanHash() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(1L);
    out.writeBoolean(true);
    out.write(filled(5), 0, 5);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());

    assertThrows(IOException.class, () -> new EvtSend<>(buf, ID_SERIALIZER, HASH_SIZE));
  }

  @Test
  @DisplayName("EvtSend reads payload and hash completely")
  void evtSendReadsAllBytes() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(1L);
    out.writeBoolean(true);
    out.write(filled(3 + HASH_SIZE), 0, 3 + HASH_SIZE);
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());

    EvtSend<TestId> actual = new EvtSend<>(buf, ID_SERIALIZER, HASH_SIZE);

    assertEquals(3, actual.getPayload().remaining());
  }
}
