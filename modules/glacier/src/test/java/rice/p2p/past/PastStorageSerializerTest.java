package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.glacier.v2.DebugContent;
import rice.p2p.past.gc.ExpiringPastMetadata;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.persistence.EmptyCache;
import rice.persistence.MemoryStorage;
import rice.persistence.StorageManagerImpl;

/**
 * Round trips of what a Past persists: its content, through the Past's own content deserializer, and GC metadata.
 */
class PastStorageSerializerTest {

  private Environment env;
  private PastryIdFactory factory;
  private PastImpl past;
  private PastStorageSerializer testObj;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    NetworkSimulator<DirectNodeHandle, RawMessage> simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    PastryNode node = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator, env).newNode();
    past = new PastImpl(node, new StorageManagerImpl(factory, new MemoryStorage(factory), new EmptyCache(factory)), 0, "past-test");
    past.setContentDeserializer((buf, endpoint, contentType) -> {
      if (contentType == DebugContent.TYPE) {
        return new DebugContent(buf, endpoint);
      }
      throw new IOException("Unknown content type: " + contentType);
    });
    testObj = new PastStorageSerializer(() -> past);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private static SimpleInputBuffer reread(SimpleOutputBuffer buf) {
    return new SimpleInputBuffer(buf.getBytes(), 0, buf.getWritten());
  }

  @Test
  void writesAndReadsContentWithThePastsDeserializer() throws IOException {
    Id id = factory.buildId("content");
    DebugContent expected = new DebugContent(id, true, 3L, new byte[] {1, 2, 3});
    SimpleOutputBuffer buf = new SimpleOutputBuffer();

    testObj.writeObject(expected, buf);
    DebugContent actual = (DebugContent) testObj.readObject(reread(buf));

    assertEquals(id, actual.getId());
    assertEquals(true, actual.isMutable());
    assertEquals(3L, actual.getVersion());
    assertArrayEquals(new byte[] {1, 2, 3}, actual.getPayload());
  }

  @Test
  void writesAndReadsGcMetadata() throws IOException {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();

    testObj.writeMetadata(new ExpiringPastMetadata(424242L), buf);

    assertEquals(new ExpiringPastMetadata(424242L), testObj.readMetadata(reread(buf)));
  }

  @Test
  void refusesOtherMetadata() {
    assertThrows(IOException.class, () -> testObj.writeMetadata("text", new SimpleOutputBuffer()));
  }
}
