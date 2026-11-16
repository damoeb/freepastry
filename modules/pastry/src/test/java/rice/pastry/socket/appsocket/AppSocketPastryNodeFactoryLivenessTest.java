package rice.pastry.socket.appsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.LivenessTypes;
import org.mpisws.p2p.transport.liveness.OverrideLiveness;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import rice.environment.Environment;
import rice.pastry.Id;
import rice.pastry.PastryNode;

class AppSocketPastryNodeFactoryLivenessTest {
  private Environment env;
  private Probe factory;

  /** Reaches the protected liveness layer, whose type is visible to subclasses only. */
  static class Probe extends AppSocketPastryNodeFactory {
    Probe(Environment env) throws IOException {
      super(() -> Id.build("1"), 0, env);
    }

    Liveness livenessOf(PastryNode pn, TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> tl) {
      TransLiveness<SourceRoute<MultiInetSocketAddress>, ByteBuffer> ret = getLivenessTransportLayer(tl, pn);
      return new Liveness(ret.getTransportLayer(), ret.getLivenessProvider(), ret.getOverrideLiveness(), ret.getPinger());
    }
  }

  record Liveness(TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> transportLayer,
      LivenessProvider<SourceRoute<MultiInetSocketAddress>> livenessProvider,
      OverrideLiveness<SourceRoute<MultiInetSocketAddress>> overrideLiveness,
      Pinger<SourceRoute<MultiInetSocketAddress>> pinger) {}

  @BeforeEach
  void setUp() throws IOException {
    env = new Environment();
    factory = new Probe(env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("the liveness layer of a socket-only remote node passes the transport layer through and does nothing")
  void theLivenessOfASocketOnlyNodeDoesNothing() {
    PastryNode remote = factory.new BogusTLPastryNode(Id.build("2"), env);
    @SuppressWarnings("unchecked") // a proxy of the raw interface, never called
    TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> tl = (TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer>)
        Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {TransportLayer.class}, (proxy, method, args) -> null);

    Liveness actual = factory.livenessOf(remote, tl);

    assertSame(tl, actual.transportLayer());
    assertEquals(LivenessTypes.LIVENESS_ALIVE, actual.livenessProvider().getLiveness(null, null));
    assertFalse(actual.livenessProvider().checkLiveness(null, null));
    assertFalse(actual.livenessProvider().removeLivenessListener(null));
    assertFalse(actual.pinger().ping(null, null));
    assertFalse(actual.pinger().removePingListener(null));
    actual.overrideLiveness().setLiveness(null, LivenessTypes.LIVENESS_DEAD, null);
    assertEquals(LivenessTypes.LIVENESS_ALIVE, actual.livenessProvider().getLiveness(null, null));
  }
}
