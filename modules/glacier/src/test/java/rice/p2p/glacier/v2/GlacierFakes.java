package rice.p2p.glacier.v2;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.persistence.StorageManager;

/** Hand-made stand-ins for the collaborators of the Glacier components under test. */
final class GlacierFakes {
  private GlacierFakes() {}

  /** A node handle that is only identified by its id. */
  static NodeHandle nodeHandle(Id id) {
    return new NodeHandle() {
      @Override
      public Id getId() {
        return id;
      }

      @Override
      public boolean checkLiveness() {
        return true;
      }

      @Override
      public void serialize(OutputBuffer buf) {}
    };
  }

  /** An endpoint that records every message routed through it. */
  static Endpoint endpoint(NodeHandle local, List<Object> routed) {
    return (Endpoint) Proxy.newProxyInstance(Endpoint.class.getClassLoader(), new Class<?>[] {Endpoint.class}, (proxy, method, args) -> {
      if (method.getName().equals("route")) {
        routed.add(args[1]);
        return null;
      }
      if (method.getName().equals("getLocalNodeHandle")) {
        return local;
      }
      return null;
    });
  }

  /** A router on top of {@link #endpoint}; the statistics collector only counts messages, so it needs no collaborators. */
  static GlacierRouter router(NodeHandle local, List<Object> routed, Environment env) {
    GlacierStatisticsCollector statistics = new GlacierStatisticsCollector(null, null, null, null, null, null, null, null, env,LoggerFactory.getLogger(GlacierFakes.class));
    return new GlacierRouter(endpoint(local, routed), statistics, LoggerFactory.getLogger(GlacierFakes.class));
  }

  /** A storage manager whose calls are answered by the given handler. */
  static StorageManager storage(InvocationHandler handler) {
    return (StorageManager) Proxy.newProxyInstance(StorageManager.class.getClassLoader(), new Class<?>[] {StorageManager.class}, handler);
  }

  static List<Object> messages() {
    return new ArrayList<>();
  }
}
