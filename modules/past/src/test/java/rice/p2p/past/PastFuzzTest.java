package rice.p2p.past;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.past.gc.ExpiringPastImpl;
import rice.p2p.past.gc.ExpiringPastMetadata;
import rice.p2p.past.testing.DistPastTestContent;
import rice.p2p.replication.ReplicationClient;
import rice.p2p.replication.ReplicationImpl;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.StubHandleNodes;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;
import rice.testing.fuzz.FuzzSupport;

/**
 * Fuzzes the wire messages of Past (with the garbage-collected variants), of replication, and the content handles and
 * metadata they carry. The invariant: a message or an {@link IOException}, nothing else.
 */
class PastFuzzTest {
  private static final int REPLICAS = 3;
  private static final long COLLECTION_INTERVAL = 100_000;

  private static Environment env;
  private static PastryNode node;
  private static MessageDeserializer past;
  private static MessageDeserializer replication;
  private static Endpoint endpoint;

  @BeforeAll
  static void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    node = StubHandleNodes.newNode(env);
    PastryIdFactory factory = new PastryIdFactory(env);
    ExpiringPastImpl gcPast = new ExpiringPastImpl(node,
        new CachedStorageImpl(factory, new MemoryStorage(factory), new NoOpCache(factory)), null, REPLICAS, "past-fuzz",
        new PastPolicy.DefaultPastPolicy(), COLLECTION_INTERVAL, null);
    gcPast.setContentDeserializer((buf, endpoint, type) -> switch (type) {
      case DistPastTestContent.TYPE -> new DistPastTestContent(buf, endpoint);
      default -> throw new IOException("Unknown content type: " + type);
    });
    past = gcPast.getEndpoint().getDeserializer();
    endpoint = node.buildEndpoint(new rice.p2p.commonapi.Application() {
      @Override
      public boolean forward(rice.p2p.commonapi.RouteMessage message) {
        return true;
      }

      @Override
      public void deliver(rice.p2p.commonapi.Id id, rice.p2p.commonapi.Message message) {
      }

      @Override
      public void update(rice.p2p.commonapi.NodeHandle handle, boolean joined) {
      }
    }, "reader");

    ReplicationClient client = (ReplicationClient) Proxy.newProxyInstance(PastFuzzTest.class.getClassLoader(),
        new Class<?>[] {ReplicationClient.class}, (proxy, method, args) -> null);
    ReplicationImpl replicationImpl = new ReplicationImpl(node, client, REPLICAS, "replication-fuzz");
    Field field = ReplicationImpl.class.getDeclaredField("endpoint");
    field.setAccessible(true);
    replication = ((Endpoint) field.get(replicationImpl)).getDeserializer();
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  @FuzzTest(maxDuration = "30s")
  void pastMessage(FuzzedDataProvider data) {
    FuzzSupport.deserialize(past, data, node.getLocalHandle());
  }

  @FuzzTest(maxDuration = "30s")
  void replicationMessage(FuzzedDataProvider data) {
    FuzzSupport.deserialize(replication, data, node.getLocalHandle());
  }

  @FuzzTest(maxDuration = "30s")
  void contentHashHandle(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new ContentHashPastContentHandle(buf, endpoint));
  }

  @FuzzTest(maxDuration = "30s")
  void gcMetadata(FuzzedDataProvider data) {
    FuzzSupport.read(data, ExpiringPastMetadata::new);
  }
}
