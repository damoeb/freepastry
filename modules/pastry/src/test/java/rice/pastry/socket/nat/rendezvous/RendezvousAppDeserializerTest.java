package rice.pastry.socket.nat.rendezvous;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.PastryNode;
import rice.pastry.testing.StubHandleNodes;

class RendezvousAppDeserializerTest {
  private Environment env;
  private PastryNode node;
  private MessageDeserializer target;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    node = StubHandleNodes.newNode(env);
    target = new RendezvousApplication(node).getDeserializer();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private SimpleInputBuffer byteBufferMsg(int length) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeByte((byte) 0); // a node handle
    out.writeInt(length);
    out.writeInt(0);
    return new SimpleInputBuffer(out.getBytes());
  }

  @Test
  void rejectsAByteBufferLongerThanTheMessage() throws IOException {
    SimpleInputBuffer buf = byteBufferMsg(Integer.MAX_VALUE);

    assertThrows(IOException.class,
        () -> target.deserialize(buf, RendezvousMessageType.BYTE_BUFFER.id(), 0, null));
  }

  @Test
  void rejectsANegativeByteBufferLength() throws IOException {
    SimpleInputBuffer buf = byteBufferMsg(-1);

    assertThrows(IOException.class,
        () -> target.deserialize(buf, RendezvousMessageType.BYTE_BUFFER.id(), 0, null));
  }

  @Test
  void rejectsAnUnknownTypeWithAnIOException() {
    assertThrows(IOException.class,
        () -> target.deserialize(new SimpleInputBuffer(new byte[0]), (short) 12345, 0, null));
  }

  @Test
  void rejectsAnUnknownByteBufferVersionWithAnIOException() {
    SimpleInputBuffer buf = new SimpleInputBuffer(new byte[] {7});

    assertThrows(IOException.class,
        () -> target.deserialize(buf, RendezvousMessageType.BYTE_BUFFER.id(), 0, null));
  }
}
