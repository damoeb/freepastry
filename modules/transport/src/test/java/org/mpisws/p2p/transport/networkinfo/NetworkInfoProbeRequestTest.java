package org.mpisws.p2p.transport.networkinfo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.fuzz.FuzzSocket;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/** A probe request makes a neighbour contact an address; it may only be the requester's own. */
class NetworkInfoProbeRequestTest {
  private static final byte HEADER_PROBE_REQUEST = 2;

  private Environment env;
  private NetworkInfoTransportLayer target;
  private final List<MultiInetSocketAddress> requested = new ArrayList<>();

  @BeforeEach
  @SuppressWarnings({"unchecked", "ReferenceEquality"})
  void setUp() {
    env = Environment.directEnvironment(1);
    InetSocketAddress local = new InetSocketAddress(InetAddress.getLoopbackAddress(), 1);
    TransportLayer<InetSocketAddress, ByteBuffer> lower = (TransportLayer<InetSocketAddress, ByteBuffer>) Proxy
        .newProxyInstance(getClass().getClassLoader(), new Class<?>[] {TransportLayer.class},
            (proxy, method, args) -> switch (method.getName()) {
              case "getLocalIdentifier" -> local;
              case "hashCode" -> System.identityHashCode(proxy);
              case "equals" -> proxy == args[0];
              case "toString" -> "lower";
              default -> null;
            });
    target = new NetworkInfoTransportLayer(lower, env, null);
    target.setCallback(new TransportLayerCallback<>() {
      @Override
      public void messageReceived(InetSocketAddress address, ByteBuffer msg, Map<String, Object> options) {}

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> socket) {}
    });
    target.setProbeStrategy(new ProbeStrategy() {
      @Override
      public Collection<InetSocketAddress> getExternalAddresses() {
        return List.of();
      }

      @Override
      public Cancellable requestProbe(MultiInetSocketAddress addr, long uid,
          Continuation<Boolean, Exception> deliverResultToMe) {
        requested.add(addr);
        deliverResultToMe.receiveResult(true);
        return () -> true;
      }
    });
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private void probeRequest(InetSocketAddress requester, MultiInetSocketAddress probeMe) throws IOException {
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    sob.writeByte(HEADER_PROBE_REQUEST);
    probeMe.serialize(sob);
    sob.writeLong(42L);
    FuzzSocket<InetSocketAddress> socket =
        new FuzzSocket<>(requester, java.util.Arrays.copyOf(sob.getBytes(), sob.getWritten()));
    target.incomingSocket(socket);
    socket.drive();
  }

  private static InetSocketAddress addr(String ip, int port) throws IOException {
    return new InetSocketAddress(InetAddress.getByName(ip), port);
  }

  @Test
  void aRequestToProbeAnotherHostIsRefused() throws IOException {
    probeRequest(addr("192.0.2.7", 40000), new MultiInetSocketAddress(addr("198.51.100.9", 22)));

    assertEquals(List.of(), requested);
  }

  @Test
  void aRequestToProbeTheRequestersOwnHostIsPassedOn() throws IOException {
    MultiInetSocketAddress own = new MultiInetSocketAddress(addr("192.0.2.7", 9001), addr("10.0.0.7", 9001));

    probeRequest(addr("192.0.2.7", 40000), own);

    assertEquals(List.of(own), requested);
  }
}
