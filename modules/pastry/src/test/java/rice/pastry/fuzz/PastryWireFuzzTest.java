package rice.pastry.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.testing.fuzz.FuzzSupport;
import rice.pastry.Id;
import rice.pastry.IdRange;
import rice.pastry.NodeSet;
import rice.pastry.leafset.LeafSet;
import rice.pastry.pns.PNSApplication;
import java.net.InetSocketAddress;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import rice.pastry.socket.SPNFIdentitySerializer;
import rice.pastry.socket.SocketNodeHandleFactory;
import rice.pastry.socket.nat.rendezvous.RendezvousSPNFIdentitySerializer;
import rice.pastry.socket.nat.probe.ProbeApp;
import rice.pastry.socket.nat.rendezvous.RendezvousApp;
import rice.pastry.socket.nat.rendezvous.RendezvousJoinRequest;
import rice.pastry.socket.nat.rendezvous.RendezvousSNHFactory;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.NodeHandleFactoryListener;
import rice.pastry.PastryNode;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.join.JoinAddress;
import rice.pastry.leafset.LeafSetProtocolAddress;
import rice.pastry.routing.RouteProtocolAddress;
import rice.pastry.routing.RouterAddress;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.pastry.transport.TLDeserializer;
import rice.p2p.commonapi.rawserialization.RawMessage;

/**
 * Fuzzes the Pastry wire entry point, the {@link TLDeserializer}: an address, a priority and a type, then the body
 * that the registered protocol applications (join, route set, leaf set, router, proximity neighbor selection) read.
 * The invariant: a value or an {@link IOException}, nothing else.
 */
class PastryWireFuzzTest {
  private static final int[] ADDRESSES = {JoinAddress.getCode(), RouteProtocolAddress.getCode(),
      LeafSetProtocolAddress.getCode(), RouterAddress.getCode()};

  private static final MultiInetSocketAddress PEER =
      new MultiInetSocketAddress(InetSocketAddress.createUnresolved("peer", 9001));

  private static Environment env;
  private static PastryNode node;
  private static TLDeserializer target;
  private static MessageDeserializer pns;
  private static MessageDeserializer rendezvous;
  private static MessageDeserializer probe;

  @BeforeAll
  static void setUp() throws IOException {
    env = Environment.directEnvironment(1);
    node = newNode(env);
    pns = new PNSApplication(node).getDeserializer();
    rendezvous = new RendezvousApp(node).getDeserializer();
    probe = new ProbeApp(node, null, null).getDeserializer();
  }

  @AfterAll
  static void tearDown() {
    env.destroy();
  }

  /** The simulator node of {@code StubHandleNodes}, which also hands out its {@link TLDeserializer}. */
  private static PastryNode newNode(Environment env) throws IOException {
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

      @Override
      protected TLDeserializer getTLDeserializer(NodeHandleFactory<?> handleFactory, PastryNode pn) {
        target = super.getTLDeserializer(handleFactory, pn);
        return target;
      }
    }.newNode();
  }

  @FuzzTest(maxDuration = "30s")
  void tlDeserializer(FuzzedDataProvider data) {
    int address = data.consumeBoolean() ? ADDRESSES[data.consumeInt(0, ADDRESSES.length - 1)] : data.consumeInt();
    byte[] rest = data.consumeRemainingAsBytes();
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    try {
      out.writeInt(address);
      out.write(rest, 0, rest.length);
      target.deserialize(new SimpleInputBuffer(out.getBytes()), node.getLocalHandle());
    } catch (IOException expected) {
      // the accepted way to reject malformed input
    }
  }

  @FuzzTest(maxDuration = "30s")
  void socketNodeHandle(FuzzedDataProvider data) {
    FuzzSupport.read(data, new SocketNodeHandleFactory(node)::readNodeHandle);
  }

  @FuzzTest(maxDuration = "30s")
  void rendezvousNodeHandle(FuzzedDataProvider data) {
    FuzzSupport.read(data, new RendezvousSNHFactory(node)::readNodeHandle);
  }

  @FuzzTest(maxDuration = "30s")
  void id(FuzzedDataProvider data) {
    FuzzSupport.read(data, Id::build);
  }

  @FuzzTest(maxDuration = "30s")
  void idRange(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new IdRange(buf));
  }

  @FuzzTest(maxDuration = "30s")
  void nodeSet(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new NodeSet(buf, node));
  }

  @FuzzTest(maxDuration = "30s")
  void leafSet(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> LeafSet.build(buf, node));
  }

  @FuzzTest(maxDuration = "30s")
  void pnsApplication(FuzzedDataProvider data) {
    FuzzSupport.deserialize(pns, data, node.getLocalHandle());
  }

  @FuzzTest(maxDuration = "30s")
  void rendezvousApp(FuzzedDataProvider data) {
    FuzzSupport.deserialize(rendezvous, data, node.getLocalHandle());
  }

  @FuzzTest(maxDuration = "30s")
  void probeApp(FuzzedDataProvider data) {
    FuzzSupport.deserialize(probe, data, node.getLocalHandle());
  }

  @FuzzTest(maxDuration = "30s")
  void rendezvousJoinRequest(FuzzedDataProvider data) {
    FuzzSupport.read(data, buf -> new RendezvousJoinRequest(buf, node, node.getLocalHandle(), node));
  }

  @FuzzTest(maxDuration = "30s")
  void identitySerializer(FuzzedDataProvider data) {
    var route = new MultiAddressSourceRouteFactory().getSourceRoute(PEER);
    var serializer = new SPNFIdentitySerializer(node, new SocketNodeHandleFactory(node));
    FuzzSupport.read(data, buf -> serializer.deserialize(buf, route));
  }

  @FuzzTest(maxDuration = "30s")
  void rendezvousIdentitySerializer(FuzzedDataProvider data) {
    var route = new MultiAddressSourceRouteFactory().getSourceRoute(PEER);
    var serializer = new RendezvousSPNFIdentitySerializer(node, new RendezvousSNHFactory(node)) {
    };
    FuzzSupport.read(data, buf -> serializer.deserialize(buf, route));
  }
}
