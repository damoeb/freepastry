package rice.pastry.testing;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryEndpoint;
import rice.pastry.commonapi.PastryEndpointMessage;
import rice.pastry.messaging.PastryRawMessage;
import rice.pastry.routing.RouteMessage;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.transport.TransportLayerDeserializer;

/**
 * Sends values and messages through the bytes a socket would carry, without a socket: the direct simulator hands
 * message objects from node to node, so it never runs serialize() or the reading constructors.
 *
 * <p>Every round trip checks three things: the reader consumes exactly the bytes that serialize() wrote, the value
 * read serializes to the same bytes again (as a hop that reads a message and sends it on does), and that second
 * copy reads back too. A field that only the sending constructor sets, or a count the reader checks against
 * different bytes than serialize() writes, fails one of them.
 */
public final class WireRoundTrip {
  private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
  private static final AtomicInteger NEXT_PORT = new AtomicInteger(1024);

  private WireRoundTrip() {
  }

  /** Reads a value from a buffer. */
  @FunctionalInterface
  public interface Reader<T> {
    T read(InputBuffer buf) throws IOException;
  }

  /** Writes a value to a buffer, for values that have a serialize(OutputBuffer) but no common interface. */
  @FunctionalInterface
  public interface Writer<T> {
    void write(T value, OutputBuffer buf) throws IOException;
  }

  /** The bytes serialize() writes. */
  public static byte[] bytes(RawSerializable value) throws IOException {
    return bytes(value, RawSerializable::serialize);
  }

  /** The bytes the writer writes. */
  public static <T> byte[] bytes(T value, Writer<? super T> writer) throws IOException {
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    writer.write(value, out);
    return out.getBytes();
  }

  /**
   * Serializes, reads back, serializes the copy and reads that again.
   *
   * @return the second copy
   */
  public static <T extends RawSerializable> T roundTrip(T value, Reader<? extends T> reader) throws IOException {
    return roundTrip(value, RawSerializable::serialize, reader);
  }

  /**
   * Like {@link #roundTrip(RawSerializable, Reader)} for a value written by a method reference such as
   * {@code LeafSet::serialize}.
   */
  public static <T> T roundTrip(T value, Writer<? super T> writer, Reader<? extends T> reader) throws IOException {
    byte[] first = bytes(value, writer);
    T copy = readAll(first, reader, value);
    byte[] second = bytes(copy, writer);
    assertArrayEquals(first, second, () -> "the copy of " + value + " serializes to the same bytes");
    return readAll(second, reader, value);
  }

  /** Like {@link #roundTrip(RawSerializable, Reader)} for a message that a {@link MessageDeserializer} reads. */
  @SuppressWarnings("unchecked")
  public static <M extends RawMessage> M roundTrip(M message, MessageDeserializer deserializer,
      rice.p2p.commonapi.NodeHandle sender) throws IOException {
    return (M) roundTrip((RawMessage) message, buf -> {
      Message read = deserializer.deserialize(buf, message.getType(), message.getPriority(), sender);
      assertNotNull(read, () -> "the deserializer read " + message);
      assertEquals(message.getClass(), read.getClass());
      return (RawMessage) read;
    });
  }

  private static <T> T readAll(byte[] bytes, Reader<T> reader, Object value) throws IOException {
    SimpleInputBuffer inputBuffer = new SimpleInputBuffer(bytes);
    T read = reader.read(inputBuffer);
    assertNotNull(read, () -> "read " + value);
    assertEquals(0, inputBuffer.bytesRemaining(), () -> "bytes left after reading " + value);
    return read;
  }

  /** A socket node that is never booted: its handles and ids read and write the way they do on a socket ring. */
  public static final class SocketNode {
    public final PastryNode node;
    private final TransportLayerDeserializer[] transportLayer;

    private SocketNode(PastryNode node, TransportLayerDeserializer[] transportLayer) {
      this.node = node;
      this.transportLayer = transportLayer;
    }

    /**
     * Builds a node with the given hex id on a free loopback port.
     */
    public static SocketNode create(Environment env, String id) throws IOException {
      TransportLayerDeserializer[] transportLayer = new TransportLayerDeserializer[1];
      int port;
      try (java.net.ServerSocket probe = new java.net.ServerSocket(0, 1, LOOPBACK)) {
        port = probe.getLocalPort();
      }
      PastryNode node = new SocketPastryNodeFactory(() -> Id.build(id), LOOPBACK, port, env) {
        @Override
        protected TransportLayerDeserializer getTransportLayerDeserializer(NodeHandleFactory<?> handleFactory, PastryNode pn) {
          transportLayer[0] = super.getTransportLayerDeserializer(handleFactory, pn);
          return transportLayer[0];
        }
      }.newNode();
      return new SocketNode(node, transportLayer);
    }

    /** A handle of another node, read the way a handle from the wire is read. */
    public NodeHandle handle(String id) throws IOException {
      SimpleOutputBuffer out = new SimpleOutputBuffer();
      new MultiInetSocketAddress(new InetSocketAddress(LOOPBACK, NEXT_PORT.incrementAndGet())).serialize(out);
      out.writeLong(NEXT_PORT.get());
      Id.build(id).serialize(out);
      return node.readNodeHandle(new SimpleInputBuffer(out.getBytes()));
    }

    /** The deserializer the node registered for an address. */
    public MessageDeserializer deserializer(int address) {
      return transportLayer[0].getDeserializer(address);
    }

    /** The node's local handle. */
    public NodeHandle local() {
      return node.getLocalHandle();
    }

    /** The bytes the transport writes for a message to a node: address, priority, type and the message. */
    public byte[] wire(RawMessage message) throws IOException {
      SimpleOutputBuffer out = new SimpleOutputBuffer();
      transportLayer[0].serialize(message, out);
      return out.getBytes();
    }

    /** Reads a message from the bytes of {@link #wire(RawMessage)}, as the node does when it arrives. */
    public RawMessage read(byte[] bytes, NodeHandle from) throws IOException {
      SimpleInputBuffer inputBuffer = new SimpleInputBuffer(bytes);
      RawMessage read = transportLayer[0].deserialize(inputBuffer, from);
      if (!(read instanceof RouteMessage)) { // a route message keeps the rest of the buffer to unwrap later
        assertEquals(0, inputBuffer.bytesRemaining(), () -> "bytes left after reading " + read);
      }
      return read;
    }

    /**
     * Sends a Pastry message (one with an address) to this node through the transport's bytes, reads it, and sends
     * the copy on again, as a hop would.
     *
     * @return the copy read at the second hop
     */
    @SuppressWarnings("unchecked")
    public <M extends PastryRawMessage> M hop(M message, NodeHandle from) throws IOException {
      byte[] first = wire(message);
      RawMessage copy = read(first, from);
      assertEquals(message.getClass(), copy.getClass());
      byte[] second = wire(copy);
      // the priority byte is not compared here; TransportLayerDeserializerPriorityTest pins it
      assertArrayEquals(withoutPriority(first), withoutPriority(second),
          () -> "the copy of " + message + " is sent on as the same bytes");
      return (M) read(second, from);
    }

    /**
     * The transport's bytes with the priority byte (after the int address) cleared. A message that is read from the
     * wire and sent on directly takes the default priority; a route message keeps the priority of the message it
     * carries, also after a hop unwraps it.
     */
    public static byte[] withoutPriority(byte[] wire) {
      byte[] copy = wire.clone();
      copy[Integer.BYTES] = 0;
      return copy;
    }

    /**
     * Routes an application message over two hops: the endpoint's application wraps it in a route message, the
     * first hop reads the route message, unwraps the application's message (as forward() does) and routes it on,
     * the second hop reads it again. A route message that the first hop sends on without unwrapping is checked
     * too.
     *
     * @return the application's message as the second hop unwraps it
     */
    @SuppressWarnings("unchecked")
    public <M extends RawMessage> M route(Endpoint endpoint, rice.p2p.commonapi.Id key, M message,
        rice.p2p.commonapi.NodeHandle sender, boolean toHandle) throws IOException {
      roundTrip(message, endpoint.getDeserializer(), sender);
      PastryEndpoint pe = (PastryEndpoint) endpoint;
      PastryEndpointMessage endpointMessage = new PastryEndpointMessage(pe.getAddress(), message, (NodeHandle) sender);
      RouteMessage routeMessage = new RouteMessage((Id) key, endpointMessage, (byte) 1);
      if (toHandle) {
        routeMessage.setDestinationHandle(handle(((Id) key).toStringFull()));
      }
      byte[] first = wire(routeMessage);
      NodeHandle prev = handle("ABCD");

      // a hop that only routes the bytes on
      RouteMessage untouched = assertInstanceOf(RouteMessage.class, read(first, prev));
      assertArrayEquals(first, wire(untouched), () -> "a hop that does not unwrap " + message + " sends the same bytes");

      // a hop whose application reads the message, then routes it on
      RouteMessage atHop = assertInstanceOf(RouteMessage.class, read(first, prev));
      Message unwrapped = atHop.getMessage(endpoint.getDeserializer());
      assertEquals(message.getClass(), unwrapped.getClass());
      assertArrayEquals(bytes(message), bytes((RawMessage) unwrapped), () -> "the unwrapped copy of " + message);
      byte[] second = wire(atHop);
      assertArrayEquals(first, second, () -> "the hop sends " + message + " on as the same bytes");

      RouteMessage atNext = assertInstanceOf(RouteMessage.class, read(second, prev));
      return (M) atNext.getMessage(endpoint.getDeserializer());
    }
  }
}
