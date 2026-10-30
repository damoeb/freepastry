package rice.p2p.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.commonapi.PastryIdFactory;

/**
 * Round trips of the metadata of the aggregation's waiting list.
 */
class AggregationStorageSerializerTest {

  private Environment env;
  private IdFactory factory;
  private AggregationStorageSerializer testObj;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    testObj = new AggregationStorageSerializer(factory, () -> null);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void writesAndReadsAnObjectDescriptor() throws IOException {
    Id key = factory.buildId("object");
    ObjectDescriptor expected = new ObjectDescriptor(key, 4L, 1000L, 2000L, 512);
    SimpleOutputBuffer buf = new SimpleOutputBuffer();

    testObj.writeMetadata(expected, buf);
    ObjectDescriptor actual = (ObjectDescriptor) testObj.readMetadata(new SimpleInputBuffer(buf.getBytes(), 0, buf.getWritten()));

    assertEquals(0, expected.compareTo(actual));
    assertEquals(key, actual.getKey());
    assertEquals(4L, actual.getVersion());
    assertEquals(1000L, actual.getCurrentLifetime());
    assertEquals(2000L, actual.getRefreshedLifetime());
    assertEquals(512, actual.getSize());
  }

  @Test
  void needsTheAggregationToReadAnObject() {
    assertThrows(IOException.class, () -> testObj.readObject(new SimpleInputBuffer(new byte[] {0, 1})));
  }
}
