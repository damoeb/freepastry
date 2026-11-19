package rice.pastry.testing;

import java.io.IOException;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.NodeHandleFactoryListener;
import rice.pastry.PastryNode;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * Builds simulator nodes that can read node handles from a buffer, which the simulator itself never does: a handle
 * on the wire is a single byte, and reads as the local handle. Lets deserializer tests get past the handles and
 * reach the lengths they want to check.
 */
public final class StubHandleNodes {
  private StubHandleNodes() {
  }

  public static PastryNode newNode(Environment env) throws IOException {
    return new DirectPastryNodeFactory(new RandomNodeIdFactory(env),
        new EuclideanNetwork<DirectNodeHandle, RawMessage>(env), env) {
      @Override
      protected NodeHandleFactory<?> getNodeHandleFactory(PastryNode pn) {
        return new NodeHandleFactory<NodeHandle>() {
          @Override
          public NodeHandle readNodeHandle(InputBuffer buf) throws IOException {
            buf.readByte();
            return pn.getLocalHandle();
          }

          @Override
          public NodeHandle coalesce(NodeHandle handle) {
            return handle;
          }

          @Override
          public void addNodeHandleFactoryListener(NodeHandleFactoryListener<? super NodeHandle> listener) {
          }

          @Override
          public void removeNodeHandleFactoryListener(NodeHandleFactoryListener<? super NodeHandle> listener) {
          }
        };
      }
    }.newNode();
  }
}
