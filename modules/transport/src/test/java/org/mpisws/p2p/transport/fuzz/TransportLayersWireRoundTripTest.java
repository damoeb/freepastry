package org.mpisws.p2p.transport.fuzz;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.multiaddress.MultiInetAddressTransportLayer;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.networkinfo.ConnectivityResult;
import org.mpisws.p2p.transport.networkinfo.NetworkInfoTransportLayer;
import org.mpisws.p2p.transport.networkinfo.ProbeStrategy;
import org.mpisws.p2p.transport.simpleidentity.InetSocketAddressSerializer;
import org.mpisws.p2p.transport.simpleidentity.SimpleIdentityTransportLayer;
import org.mpisws.p2p.transport.table.TableStore;
import org.mpisws.p2p.transport.table.TableTransprotLayerImpl;
import org.mpisws.p2p.transport.table.UnknownValueException;
import org.mpisws.p2p.transport.util.Serializer;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Sends what the socket and message layers of the transport stack write to the layer of a peer, over the in-memory
 * {@link PipeNetwork}: the bytes a layer writes are the only thing the peer's layer sees, so a header that the writer
 * and the reader disagree on, or a field only the writing side sets, fails here and not only on a real socket.
 */
class TransportLayersWireRoundTripTest {
  private static final int MAX_EXTERNAL_ADDRESSES = 20;
  private static final int MAX_ID_SIZE = 64 * 1024;

  private Environment env;
  private PipeNetwork net;
  private final List<Object> errors = new ArrayList<>();

  private final ErrorHandler<InetSocketAddress> inetErrors = new ErrorHandler<>() {
    @Override
    public void receivedUnexpectedData(InetSocketAddress i, byte[] bytes, int location, Map<String, Object> options) {
      errors.add("unexpected data from " + i + " at " + location);
    }

    @Override
    public void receivedException(InetSocketAddress i, Throwable error) {
      errors.add(error);
    }
  };

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    net = new PipeNetwork();
  }

  @AfterEach
  void tearDown() {
    assertEquals(List.of(), errors, "errors reported by the layers");
    assertEquals(List.of(), net.failures(), "what the layers threw");
    env.destroy();
  }

  private static InetSocketAddress addr(int lastOctet, int port) {
    try {
      return new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, (byte) lastOctet}), port);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static InetSocketAddress addr6(int lastByte, int port) throws IOException {
    byte[] bytes = new byte[16];
    bytes[0] = 0x20;
    bytes[1] = 0x01;
    bytes[15] = (byte) lastByte;
    return new InetSocketAddress(InetAddress.getByAddress(bytes), port);
  }

  /** Records what the layer above receives. */
  private static final class Recorder<I> implements TransportLayerCallback<I, ByteBuffer> {
    final List<I> messageSenders = new ArrayList<>();
    final List<byte[]> messages = new ArrayList<>();
    final List<I> socketPeers = new ArrayList<>();
    final List<P2PSocket<I>> sockets = new ArrayList<>();

    @Override
    public void messageReceived(I i, ByteBuffer m, Map<String, Object> options) {
      messageSenders.add(i);
      byte[] bytes = new byte[m.remaining()];
      m.get(bytes);
      messages.add(bytes);
    }

    @Override
    public void incomingSocket(P2PSocket<I> s) {
      socketPeers.add(s.getIdentifier());
      sockets.add(s);
    }
  }

  private static byte[] payload(int length) {
    byte[] b = new byte[length];
    for (int i = 0; i < length; i++) {
      b[i] = (byte) (i * 31 + 5);
    }
    return b;
  }

  // ---- networkinfo

  private NetworkInfoTransportLayer networkInfo(InetSocketAddress address) {
    return new NetworkInfoTransportLayer(net.lower(address), env, inetErrors);
  }

  private static ProbeStrategy strategy(Collection<InetSocketAddress> external) {
    return new ProbeStrategy() {
      @Override
      public Cancellable requestProbe(MultiInetSocketAddress addr, long uid,
          Continuation<Boolean, Exception> deliverResultToMe) {
        deliverResultToMe.receiveResult(true);
        return null;
      }

      @Override
      public Collection<InetSocketAddress> getExternalAddresses() {
        return external;
      }
    };
  }

  @Test
  void networkInfoPassesMessagesThroughWithItsHeader() throws IOException {
    NetworkInfoTransportLayer a = networkInfo(addr(1, 9001));
    NetworkInfoTransportLayer b = networkInfo(addr(2, 9002));
    Recorder<InetSocketAddress> received = new Recorder<>();
    b.setCallback(received);

    a.sendMessage(addr(2, 9002), ByteBuffer.wrap(payload(100)), null, null);
    net.run();

    assertEquals(List.of(addr(1, 9001)), received.messageSenders);
    assertArrayEquals(payload(100), received.messages.get(0));
  }

  @Test
  void networkInfoAnswersTheAddressOfTheAsker() throws IOException {
    NetworkInfoTransportLayer a = networkInfo(addr(1, 9001));
    networkInfo(addr(2, 9002));
    AtomicReference<InetSocketAddress> answer = new AtomicReference<>();

    a.getMyInetAddress(addr(2, 9002), result(answer), null);
    net.run();

    assertEquals(addr(1, 9001).getAddress(), answer.get().getAddress());
    assertTrue(answer.get().getPort() >= PipeNetwork.EPHEMERAL_PORT_BASE);
  }

  @Test
  void networkInfoAnswersTheAddressOfAnIpv6Asker() throws IOException {
    NetworkInfoTransportLayer a = networkInfo(addr6(1, 9001));
    networkInfo(addr6(2, 9002));
    AtomicReference<InetSocketAddress> answer = new AtomicReference<>();

    a.getMyInetAddress(addr6(2, 9002), result(answer), null);
    net.run();

    assertEquals(addr6(1, 9001).getAddress(), answer.get().getAddress());
  }

  private static <T> Continuation<T, IOException> result(AtomicReference<T> into) {
    return new Continuation<>() {
      @Override
      public void receiveResult(T result) {
        into.set(result);
      }

      @Override
      public void receiveException(IOException exception) {
        throw new AssertionError(exception);
      }
    };
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 19, MAX_EXTERNAL_ADDRESSES})
  @DisplayName("NetworkInfo answers its external addresses, none up to the most it sends")
  void networkInfoAnswersExternalNodes(int count) throws IOException {
    NetworkInfoTransportLayer a = networkInfo(addr(1, 9001));
    NetworkInfoTransportLayer b = networkInfo(addr(2, 9002));
    List<InetSocketAddress> external = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      external.add(i % 2 == 0 ? addr(10 + i, 5000 + i) : addr6(10 + i, 5000 + i));
    }
    b.setProbeStrategy(strategy(external));
    AtomicReference<Collection<InetSocketAddress>> answer = new AtomicReference<>();

    a.getExternalNodes(addr(2, 9002), result(answer), null);
    net.run();

    assertEquals(external, new ArrayList<>(answer.get()));
  }

  @Test
  @DisplayName("NetworkInfo with more external addresses than it sends answers the first ones")
  void networkInfoTruncatesExternalNodes() throws IOException {
    NetworkInfoTransportLayer a = networkInfo(addr(1, 9001));
    NetworkInfoTransportLayer b = networkInfo(addr(2, 9002));
    List<InetSocketAddress> external = new ArrayList<>();
    for (int i = 0; i < MAX_EXTERNAL_ADDRESSES + 5; i++) {
      external.add(addr(10 + i, 5000 + i));
    }
    b.setProbeStrategy(strategy(external));
    AtomicReference<Collection<InetSocketAddress>> answer = new AtomicReference<>();

    a.getExternalNodes(addr(2, 9002), result(answer), null);
    net.run();

    assertEquals(external.subList(0, MAX_EXTERNAL_ADDRESSES), new ArrayList<>(answer.get()));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 255, 70, MAX_ID_SIZE})
  @DisplayName("NetworkInfo answers the id of an index: empty, short and the largest the reader accepts")
  void networkInfoAnswersAnId(int size) throws IOException {
    NetworkInfoTransportLayer a = networkInfo(addr(1, 9001));
    NetworkInfoTransportLayer b = networkInfo(addr(2, 9002));
    b.setId((byte) 7, payload(size));
    AtomicReference<byte[]> answer = new AtomicReference<>();

    a.getId(addr(2, 9002), (byte) 7, result(answer), null);
    net.run();

    assertArrayEquals(payload(size), answer.get());
  }

  @Test
  @DisplayName("NetworkInfo probe: the probed node answers over UDP and TCP with the uid of the request")
  void networkInfoProbesBack() throws IOException {
    InetSocketAddress aAddress = addr(1, 9001);
    InetSocketAddress bAddress = addr(2, 9002);
    NetworkInfoTransportLayer a = networkInfo(aAddress);
    NetworkInfoTransportLayer b = networkInfo(bAddress);
    List<Long> probed = new ArrayList<>();
    b.setProbeStrategy(new ProbeStrategy() {
      @Override
      public Cancellable requestProbe(MultiInetSocketAddress addr, long uid,
          Continuation<Boolean, Exception> deliverResultToMe) {
        // the third node of a real ring; here the probed node answers on the address the request carries
        assertEquals(new MultiInetSocketAddress(aAddress, addr(50, 50)), addr);
        b.probe(addr.getOutermostAddress(), uid, new Continuation<Long, Exception>() {
          @Override
          public void receiveResult(Long result) {
            probed.add(result);
            deliverResultToMe.receiveResult(true);
          }

          @Override
          public void receiveException(Exception exception) {
            deliverResultToMe.receiveException(exception);
          }
        }, null);
        return null;
      }

      @Override
      public Collection<InetSocketAddress> getExternalAddresses() {
        return List.of();
      }
    });
    List<String> events = new ArrayList<>();

    a.verifyConnectivity(new MultiInetSocketAddress(aAddress, addr(50, 50)), bAddress, new ConnectivityResult() {
      @Override
      public void udpSuccess(InetSocketAddress from, Map<String, Object> options) {
        events.add("udp " + from);
      }

      @Override
      public void tcpSuccess(InetSocketAddress from, Map<String, Object> options) {
        events.add("tcp " + from.getAddress());
      }

      @Override
      public void receiveException(Exception e) {
        events.add("failed " + e);
      }
    }, null);
    net.run();

    assertEquals(1, probed.size());
    assertEquals(2, events.size(), events::toString);
    assertTrue(events.contains("udp " + bAddress), events::toString);
    assertTrue(events.contains("tcp " + bAddress.getAddress()), events::toString);
  }

  // ---- multiaddress

  private MultiInetAddressTransportLayer multi(InetSocketAddress... local) {
    return new MultiInetAddressTransportLayer(new MultiInetSocketAddress(local), net.lower(local[0]),
        env, null, null);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  @DisplayName("MultiInetAddress layer: a message carries the sender's addresses, one and as many as the default allows")
  void multiInetAddressMessage(int addressCount) throws IOException {
    InetSocketAddress[] aAddresses = new InetSocketAddress[addressCount];
    InetSocketAddress[] bAddresses = new InetSocketAddress[addressCount];
    for (int i = 0; i < addressCount; i++) {
      aAddresses[i] = addr(1 + i * 10, 9001);
      bAddresses[i] = i % 2 == 0 ? addr(2 + i * 10, 9002) : addr6(2 + i * 10, 9002);
    }
    MultiInetAddressTransportLayer a = multi(aAddresses);
    MultiInetAddressTransportLayer b = multi(bAddresses);
    Recorder<MultiInetSocketAddress> received = new Recorder<>();
    b.setCallback(received);

    a.sendMessage(b.getLocalIdentifier(), ByteBuffer.wrap(payload(33)), null, null);
    net.run();

    assertEquals(List.of(new MultiInetSocketAddress(aAddresses)), received.messageSenders);
    assertEquals(addressCount, received.messageSenders.get(0).getNumAddresses());
    assertArrayEquals(payload(33), received.messages.get(0));
  }

  @Test
  void multiInetAddressSocket() throws IOException {
    MultiInetAddressTransportLayer a = multi(addr(1, 9001), addr(11, 9001));
    MultiInetAddressTransportLayer b = multi(addr(2, 9002));
    Recorder<MultiInetSocketAddress> received = new Recorder<>();
    b.setCallback(received);
    List<P2PSocket<MultiInetSocketAddress>> opened = new ArrayList<>();

    a.openSocket(b.getLocalIdentifier(), new SocketCallback<>() {
      @Override
      public void receiveResult(SocketRequestHandle<MultiInetSocketAddress> cancellable,
          P2PSocket<MultiInetSocketAddress> sock) {
        opened.add(sock);
      }

      @Override
      public void receiveException(SocketRequestHandle<MultiInetSocketAddress> s, Exception ex) {
        throw new AssertionError(ex);
      }
    }, null);
    net.run();

    assertEquals(1, opened.size());
    assertEquals(List.of(a.getLocalIdentifier()), received.socketPeers);
  }

  // ---- simpleidentity

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @DisplayName("SimpleIdentity layer: the opener of a socket is the address it writes first, v4 and v6")
  void simpleIdentitySocketCarriesTheOpener(boolean ipv6) throws IOException {
    InetSocketAddress aAddress = ipv6 ? addr6(1, 9001) : addr(1, 9001);
    InetSocketAddress bAddress = ipv6 ? addr6(2, 9002) : addr(2, 9002);
    InetSocketAddressSerializer serializer = new InetSocketAddressSerializer();
    SimpleIdentityTransportLayer<InetSocketAddress, ByteBuffer> a = new SimpleIdentityTransportLayer<>(
        net.lower(aAddress), serializer, null, env, inetErrors);
    SimpleIdentityTransportLayer<InetSocketAddress, ByteBuffer> b = new SimpleIdentityTransportLayer<>(
        net.lower(bAddress), serializer, null, env, inetErrors);
    Recorder<InetSocketAddress> received = new Recorder<>();
    b.setCallback(received);
    List<P2PSocket<InetSocketAddress>> opened = new ArrayList<>();

    a.openSocket(bAddress, new SocketCallback<>() {
      @Override
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> sock) {
        opened.add(sock);
      }

      @Override
      public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception ex) {
        throw new AssertionError(ex);
      }
    }, null);
    net.run();

    assertEquals(1, opened.size());
    assertEquals(List.of(aAddress), received.socketPeers);
  }

  // ---- table

  private static final Serializer<Long> KEYS = new Serializer<>() {
    @Override
    public void serialize(Long i, OutputBuffer buf) throws IOException {
      buf.writeLong(i);
    }

    @Override
    public Long deserialize(InputBuffer buf) throws IOException {
      return buf.readLong();
    }
  };

  private static final Serializer<byte[]> VALUES = new Serializer<>() {
    @Override
    public void serialize(byte[] i, OutputBuffer buf) throws IOException {
      buf.writeInt(i.length);
      buf.write(i, 0, i.length);
    }

    @Override
    public byte[] deserialize(InputBuffer buf) throws IOException {
      byte[] ret = new byte[buf.readInt()];
      buf.read(ret);
      return ret;
    }
  };

  private static final class MapStore implements TableStore<Long, byte[]> {
    final Map<Long, byte[]> map = new java.util.HashMap<>();

    @Override
    public boolean containsKey(Long key) {
      return map.containsKey(key);
    }

    @Override
    public byte[] put(Long k, byte[] v) {
      return map.put(k, v);
    }

    @Override
    public byte[] get(Long k) {
      return map.get(k);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 100, 70_000})
  @DisplayName("Table layer: a requested value is the bytes the peer's table answers, empty to larger than 64 KiB")
  void tableRequestsAValue(int size) throws IOException {
    MapStore clientStore = new MapStore();
    MapStore serverStore = new MapStore();
    serverStore.put(Long.MIN_VALUE, payload(size));
    TableTransprotLayerImpl<InetSocketAddress, Long, byte[]> client = new TableTransprotLayerImpl<>(KEYS, VALUES,
        clientStore, net.lower(addr(1, 9001)), env);
    TableTransprotLayerImpl<InetSocketAddress, Long, byte[]> server = new TableTransprotLayerImpl<>(KEYS, VALUES,
        serverStore, net.lower(addr(2, 9002)), env);
    net.lower(addr(1, 9001)).setCallback(client);
    net.lower(addr(2, 9002)).setCallback(server);
    AtomicReference<byte[]> answer = new AtomicReference<>();
    AtomicReference<Exception> failure = new AtomicReference<>();

    client.requestValue(addr(2, 9002), Long.MIN_VALUE, new Continuation<>() {
      @Override
      public void receiveResult(byte[] result) {
        answer.set(result);
      }

      @Override
      public void receiveException(Exception exception) {
        failure.set(exception);
      }
    }, null);
    net.run();

    assertEquals(null, failure.get());
    assertArrayEquals(payload(size), answer.get());
    assertArrayEquals(payload(size), clientStore.get(Long.MIN_VALUE));
  }

  @Test
  void tableAnswersAnUnknownKey() throws IOException {
    TableTransprotLayerImpl<InetSocketAddress, Long, byte[]> client = new TableTransprotLayerImpl<>(KEYS, VALUES,
        new MapStore(), net.lower(addr(1, 9001)), env);
    TableTransprotLayerImpl<InetSocketAddress, Long, byte[]> server = new TableTransprotLayerImpl<>(KEYS, VALUES,
        new MapStore(), net.lower(addr(2, 9002)), env);
    net.lower(addr(1, 9001)).setCallback(client);
    net.lower(addr(2, 9002)).setCallback(server);
    AtomicReference<Exception> failure = new AtomicReference<>();

    client.requestValue(addr(2, 9002), 42L, new Continuation<>() {
      @Override
      public void receiveResult(byte[] result) {
        throw new AssertionError("unexpected value");
      }

      @Override
      public void receiveException(Exception exception) {
        failure.set(exception);
      }
    }, null);
    net.run();

    assertNotNull(failure.get());
    assertInstanceOf(UnknownValueException.class, failure.get());
  }

  @Test
  @DisplayName("Table layer: a socket opened through it passes through to the peer's callback")
  void tablePassesSocketsThrough() throws IOException {
    TableTransprotLayerImpl<InetSocketAddress, Long, byte[]> client = new TableTransprotLayerImpl<>(KEYS, VALUES,
        new MapStore(), net.lower(addr(1, 9001)), env);
    TableTransprotLayerImpl<InetSocketAddress, Long, byte[]> server = new TableTransprotLayerImpl<>(KEYS, VALUES,
        new MapStore(), net.lower(addr(2, 9002)), env);
    net.lower(addr(1, 9001)).setCallback(client);
    net.lower(addr(2, 9002)).setCallback(server);
    Recorder<InetSocketAddress> received = new Recorder<>();
    server.setCallback(received);
    AtomicReference<P2PSocket<InetSocketAddress>> opened = new AtomicReference<>();

    client.openSocket(addr(2, 9002), new SocketCallback<>() {
      @Override
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable, P2PSocket<InetSocketAddress> sock) {
        opened.set(sock);
      }

      @Override
      public void receiveException(SocketRequestHandle<InetSocketAddress> s, Exception ex) {
        throw new AssertionError(ex);
      }
    }, null);
    net.run();

    assertNotNull(opened.get());
    assertEquals(1, received.sockets.size());
  }

  /** The bytes of a serialized value, for building the wire by hand. */
  @SuppressWarnings("unused")
  private static byte[] serialized(MultiInetSocketAddress address) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    address.serialize(out);
    return out.getBytes();
  }
}
