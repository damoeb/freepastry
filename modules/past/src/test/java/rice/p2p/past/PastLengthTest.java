package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.past.messaging.ContinuationMessage;
import rice.p2p.past.messaging.GCRefreshMessage;
import rice.p2p.replication.messaging.ReplicationResponseMessage;
import rice.p2p.replication.messaging.RequestMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * The lengths and counts in Past and replication messages come from the peer: a few bytes must not make us
 * allocate gigabytes.
 */
class PastLengthTest {
  private static final int HUGE = Integer.MAX_VALUE;

  private static final Id ID = (Id) Proxy.newProxyInstance(Id.class.getClassLoader(), new Class<?>[] {Id.class},
      (proxy, method, args) -> {
        throw new UnsupportedOperationException(method.getName());
      });

  /** An endpoint whose ids and node handles take no bytes, so a test only writes the lengths. */
  private static final Endpoint ENDPOINT = (Endpoint) Proxy.newProxyInstance(Endpoint.class.getClassLoader(),
      new Class<?>[] {Endpoint.class}, (proxy, method, args) -> switch (method.getName()) {
        case "readId" -> ID;
        case "readNodeHandle" -> null;
        default -> throw new UnsupportedOperationException(method.getName());
      });

  private interface Reader {
    void read(SimpleInputBuffer buf) throws IOException;
  }

  private static void assertRejected(SimpleOutputBuffer out, Reader reader) {
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());
    assertThrows(IOException.class, () -> reader.read(buf));
  }

  /** The version, then the fields every PastMessage starts with, then the ContinuationMessage's serialization type. */
  private static SimpleOutputBuffer gcRefresh(byte serType) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeInt(0); // message id
    out.writeShort((short) 0); // the destination's id type
    out.writeBoolean(false); // is response
    out.writeByte(serType);
    return out;
  }

  @Test
  void gcRefreshRejectsAHugeKeyCount() throws IOException {
    SimpleOutputBuffer out = gcRefresh((byte) 0);
    out.writeInt(HUGE);

    assertRejected(out, buf -> GCRefreshMessage.build(buf, ENDPOINT));
  }

  @Test
  void gcRefreshRejectsAHugeSubArray() throws IOException {
    SimpleOutputBuffer out = gcRefresh(ContinuationMessage.S_SUB);
    out.writeInt(HUGE);

    assertRejected(out, buf -> GCRefreshMessage.build(buf, ENDPOINT));
  }

  @Test
  void requestRejectsAHugeFilterCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeInt(HUGE);

    assertRejected(out, buf -> RequestMessage.build(buf, ENDPOINT));
  }

  @Test
  void requestRejectsAHugeRangeCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeInt(0); // filters
    out.writeInt(HUGE);

    assertRejected(out, buf -> RequestMessage.build(buf, ENDPOINT));
  }

  @Test
  void responseRejectsAHugeIdSetCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeInt(HUGE);

    assertRejected(out, buf -> ReplicationResponseMessage.build(buf, ENDPOINT));
  }

  @Test
  void responseRejectsAHugeIdCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeInt(1); // id sets
    out.writeInt(HUGE);

    assertRejected(out, buf -> ReplicationResponseMessage.build(buf, ENDPOINT));
  }

  @Test
  void responseRejectsAHugeRangeCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeInt(0); // id sets
    out.writeInt(HUGE);

    assertRejected(out, buf -> ReplicationResponseMessage.build(buf, ENDPOINT));
  }
}
