package rice.p2p.scribe;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleListener;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * Minimal stand-ins for the overlay: an {@link Endpoint} that records routed messages and a
 * handle that is equal to any other handle with the same id.
 */
@SuppressWarnings("ReferenceEquality") // proxies are compared by identity
final class ScribeTestSupport {

  private ScribeTestSupport() {}

  static Id id(int n) {
    return rice.pastry.Id.build(new int[] { n, 0, 0, 0, 0 });
  }

  static Topic topic(int n) {
    return new Topic(id(1000 + n));
  }

  static final class Handle extends NodeHandle {
    private final Id id;

    Handle(int n) {
      this.id = id(n);
    }

    @Override
    public Id getId() {
      return id;
    }

    @Override
    public boolean checkLiveness() {
      return true;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Handle handle && handle.id.equals(id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    @Override
    public String toString() {
      return "Handle[" + id + "]";
    }
  }

  private static final CancellableTask NOOP_TASK = new CancellableTask() {
    @Override
    public boolean cancel() {
      return true;
    }

    @Override
    public void run() {}

    @Override
    public long scheduledExecutionTime() {
      return 0;
    }
  };

  record Routed(Id key, Object message, NodeHandle hint) {}

  /** An endpoint that is always alive, records routed messages and has the local node as replica for root topics. */
  static final class FakeEndpoint {
    final Handle local;
    final Handle remote = new Handle(2);
    final Set<Topic> rootTopics = new HashSet<>();
    final List<Routed> routed = new ArrayList<>();
    final Endpoint endpoint;

    FakeEndpoint(Handle local) {
      this.local = local;
      this.endpoint = (Endpoint) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Endpoint.class },
          (proxy, method, args) -> switch (method.getName()) {
            case "isAlive" -> true;
            case "getId" -> local.getId();
            case "getLocalNodeHandle" -> local;
            case "replicaSet" -> replicaSet((Id) args[0]);
            case "route" -> {
              routed.add(new Routed((Id) args[0], args[1], (NodeHandle) args[2]));
              yield null;
            }
            case "scheduleMessage" -> NOOP_TASK;
            case "toString" -> "FakeEndpoint";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new UnsupportedOperationException(method.toString());
          });
    }

    private NodeHandleSet replicaSet(Id key) {
      NodeHandle first = rootTopics.contains(new Topic(key)) ? local : remote;
      return (NodeHandleSet) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { NodeHandleSet.class },
          (proxy, method, args) -> switch (method.getName()) {
            case "size" -> 1;
            case "getHandle" -> first;
            case "toString" -> "ReplicaSet";
            default -> throw new UnsupportedOperationException(method.toString());
          });
    }
  }

  static NodeHandleListener listener() {
    return (NodeHandleListener) Proxy.newProxyInstance(ScribeTestSupport.class.getClassLoader(),
        new Class<?>[] { NodeHandleListener.class }, (proxy, method, args) -> switch (method.getName()) {
          case "hashCode" -> System.identityHashCode(proxy);
          case "equals" -> proxy == args[0];
          case "toString" -> "Listener";
          default -> null;
        });
  }
}
