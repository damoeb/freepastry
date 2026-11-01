package rice.p2p.glacier.v2;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;
import rice.p2p.aggregation.Aggregate;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.p2p.glacier.v2.messaging.GlacierNeighborResponseMessage;
import rice.p2p.glacier.v2.messaging.GlacierQueryMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshCompleteMessage;
import rice.p2p.glacier.v2.messaging.GlacierRefreshPatchMessage;
import rice.p2p.glacier.v2.messaging.GlacierResponseMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * The lengths and counts in Glacier messages come from the peer: a few bytes must not make us allocate gigabytes.
 */
class GlacierLengthTest {
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

  /** The version, then the fields every GlacierMessage starts with. */
  private static SimpleOutputBuffer message() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeInt(0); // message id
    out.writeChar('a'); // tag
    out.writeBoolean(false); // is response
    out.writeShort((short) 0); // the destination's id type
    return out;
  }

  private static void assertRejected(SimpleOutputBuffer out, Reader reader) {
    SimpleInputBuffer buf = new SimpleInputBuffer(out.getBytes());
    assertThrows(IOException.class, () -> reader.read(buf));
  }

  private interface Reader {
    void read(SimpleInputBuffer buf) throws IOException;
  }

  @Test
  void queryRejectsAHugeKeyCount() throws IOException {
    SimpleOutputBuffer out = message();
    out.writeInt(HUGE);

    assertRejected(out, buf -> GlacierQueryMessage.build(buf, ENDPOINT));
  }

  @Test
  void responseRejectsAHugeLifetimeCount() throws IOException {
    SimpleOutputBuffer out = message();
    out.writeInt(0); // keys
    out.writeInt(HUGE);

    assertRejected(out, buf -> GlacierResponseMessage.build(buf, ENDPOINT));
  }

  @Test
  void responseRejectsANegativeKeyCount() throws IOException {
    SimpleOutputBuffer out = message();
    out.writeInt(-1);

    assertRejected(out, buf -> GlacierResponseMessage.build(buf, ENDPOINT));
  }

  @Test
  void dataRejectsAHugeFragmentCount() throws IOException {
    SimpleOutputBuffer out = message();
    out.writeInt(HUGE);

    assertRejected(out, buf -> GlacierDataMessage.build(buf, ENDPOINT));
  }

  @Test
  void refreshCompleteRejectsAHugeUpdateCount() throws IOException {
    SimpleOutputBuffer out = message();
    out.writeInt(HUGE);

    assertRejected(out, buf -> GlacierRefreshCompleteMessage.build(buf, ENDPOINT));
  }

  @Test
  void refreshPatchRejectsAHugeSignatureLength() throws IOException {
    SimpleOutputBuffer out = message();
    out.writeInt(0); // lifetimes
    out.writeInt(0); // keys
    out.writeInt(1); // signatures
    out.writeInt(HUGE);

    assertRejected(out, buf -> GlacierRefreshPatchMessage.build(buf, ENDPOINT));
  }

  @Test
  void neighborResponseRejectsAHugeNeighborCount() throws IOException {
    SimpleOutputBuffer out = message();
    out.writeInt(0); // last seen
    out.writeInt(HUGE);

    assertRejected(out, buf -> GlacierNeighborResponseMessage.build(buf, ENDPOINT));
  }

  @Test
  void neighborResponseWithFewerNeighborsThanLastSeenIsRead() throws IOException {
    SimpleOutputBuffer out = message();
    out.writeInt(1); // last seen
    out.writeLong(0);
    out.writeInt(0); // neighbors

    GlacierNeighborResponseMessage.build(new SimpleInputBuffer(out.getBytes()), ENDPOINT);
  }

  @Test
  void fragmentRejectsAHugePayload() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeInt(HUGE);

    assertRejected(out, Fragment::new);
  }

  @Test
  void bloomFilterRejectsAHugeBitfield() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeInt(0); // hash parameters
    out.writeInt(HUGE);

    assertRejected(out, BloomFilter::new);
  }

  @Test
  void bloomFilterRejectsAHugeParameterCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeInt(HUGE);

    assertRejected(out, BloomFilter::new);
  }

  @Test
  void manifestRejectsAHugeObjectHash() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(0); // expiration
    out.writeInt(HUGE);
    out.writeInt(0);
    out.writeInt(0);
    out.writeInt(0);

    assertRejected(out, Manifest::new);
  }

  @Test
  void manifestRejectsHashesWhoseProductOverflows() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(0); // expiration
    out.writeInt(0);
    out.writeInt(1 << 16);
    out.writeInt(1 << 16);
    out.writeInt(0);

    assertRejected(out, Manifest::new);
  }

  @Test
  void manifestRejectsHugeRowsOfNoBytes() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeLong(0); // expiration
    out.writeInt(0);
    out.writeInt(1 << 30);
    out.writeInt(0);
    out.writeInt(0);

    assertRejected(out, Manifest::new);
  }

  @Test
  void debugContentRejectsAHugePayload() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeShort((short) 0); // id type
    out.writeBoolean(false);
    out.writeLong(0);
    out.writeInt(HUGE);

    assertRejected(out, buf -> new DebugContent(buf, ENDPOINT));
  }

  @Test
  void aggregateRejectsANegativePointerCount() throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeByte((byte) 0); // version
    out.writeShort((short) 0); // id type
    out.writeShort((short) -1);

    assertRejected(out, buf -> new Aggregate(buf, ENDPOINT, null));
  }
}
