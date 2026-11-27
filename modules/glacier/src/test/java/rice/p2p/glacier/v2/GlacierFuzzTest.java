package rice.p2p.glacier.v2;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import rice.environment.Environment;
import rice.p2p.aggregation.Aggregate;
import rice.p2p.aggregation.AggregateHandle;
import rice.p2p.aggregation.ObjectDescriptor;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.VersionKey;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.StubHandleNodes;
import rice.testing.fuzz.FuzzSupport;

/**
 * Fuzzes everything Glacier and the aggregation layer read from the wire: the Glacier protocol messages and the
 * fragments, manifests, keys and bloom filters they carry, and the aggregates and their descriptors. The invariant: a
 * value or an {@link IOException}, nothing else.
 */
class GlacierFuzzTest {
  private static Environment env;
  private static PastryNode node;
  private static PastryIdFactory idFactory;
  private static Endpoint endpoint;
  private static MessageDeserializer glacier;

  @BeforeAll
  static void setUp() throws IOException {
    env = Environment.directEnvironment(1);
    node = StubHandleNodes.newNode(env);
    idFactory = new PastryIdFactory(env);
    endpoint = node.buildEndpoint(new Application() {
      @Override
      public boolean forward(RouteMessage message) {
        return true;
      }

      @Override
      public void deliver(Id id, Message message) {
      }

      @Override
      public void update(NodeHandle handle, boolean joined) {
      }
    }, "fuzz");
    glacier = GlacierMessageDispatcher.deserializer(endpoint);
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  @FuzzTest(maxDuration = "30s")
  void glacierMessage(FuzzedDataProvider data) {
    FuzzSupport.deserialize(glacier, data, node.getLocalHandle());
  }

  @FuzzTest(maxDuration = "30s")
  void fragment(FuzzedDataProvider data) {
    FuzzSupport.read(data, Fragment::new);
  }

  @FuzzTest(maxDuration = "30s")
  void manifest(FuzzedDataProvider data) {
    FuzzSupport.read(data, Manifest::new);
  }

  @FuzzTest(maxDuration = "30s")
  void fragmentAndManifest(FuzzedDataProvider data) {
    FuzzSupport.read(data, FragmentAndManifest::new);
  }

  @FuzzTest(maxDuration = "30s")
  void fragmentMetadata(FuzzedDataProvider data) {
    FuzzSupport.read(data, FragmentMetadata::new);
  }

  @FuzzTest(maxDuration = "30s")
  void bloomFilter(FuzzedDataProvider data) {
    // the node probes a peer's filter with the keys of its own fragments
    FuzzSupport.read(data, buf -> new FragmentBloomFilter(buf).contains(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}));
  }

  @FuzzTest(maxDuration = "30s")
  void fragmentKey(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new FragmentKey(buf, endpoint));
  }

  @FuzzTest(maxDuration = "30s")
  void versionKey(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new VersionKey(buf, endpoint));
  }

  @FuzzTest(maxDuration = "30s")
  void debugContent(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new DebugContent(buf, endpoint));
  }

  @FuzzTest(maxDuration = "30s")
  void aggregate(FuzzedDataProvider data) {
    PastContentDeserializer contents = (buf, ep, type) -> {
      if (type == DebugContent.TYPE) {
        return new DebugContent(buf, ep);
      }
      throw new IOException("Unknown content type: " + type);
    };
    FuzzSupport.read(data, buf -> new Aggregate(buf, endpoint, contents));
  }

  @FuzzTest(maxDuration = "30s")
  void aggregateHandle(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new AggregateHandle(buf, endpoint));
  }

  @FuzzTest(maxDuration = "30s")
  void objectDescriptor(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new ObjectDescriptor(buf, idFactory));
  }
}
