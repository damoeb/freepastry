package rice.pastry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.p2p.commonapi.exception.AppNotRegisteredException;
import rice.p2p.commonapi.exception.AppSocketException;
import rice.p2p.commonapi.exception.NoReceiverAvailableException;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.client.PastryAppl;
import rice.pastry.testing.StubHandleNodes;

/** Pins the handshake that opens an application socket between two nodes: the connector side and the acceptor side. */
class AppSocketHandshakeTest {
  private static final byte ANSWER_OK = 0;
  private static final byte ANSWER_NO_APP = 1;
  private static final byte ANSWER_NO_ACCEPTOR = 2;

  private Environment env;
  private Environment connectorEnv;
  private PastryNode node;

  /** A socket that runs a registered receiver at once, reads what it is given and records what is written to it. */
  static final class ScriptedSocket implements P2PSocket<NodeHandle> {
    private final ByteBuffer toRead;
    final ByteBuffer written = ByteBuffer.allocate(16);
    boolean closed;

    ScriptedSocket(byte... toRead) {
      this.toRead = ByteBuffer.wrap(toRead);
    }

    @Override
    public @Nullable Map<String, Object> getOptions() {
      return null;
    }

    @Override
    public NodeHandle getIdentifier() {
      return null;
    }

    @Override
    public long read(ByteBuffer destination) {
      int count = Math.min(destination.remaining(), toRead.remaining());
      for (int i = 0; i < count; i++) {
        destination.put(toRead.get());
      }
      return count;
    }

    @Override
    public long write(ByteBuffer source) {
      int count = source.remaining();
      written.put(source);
      return count;
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<NodeHandle> receiver) {
      if (receiver == null) {
        return;
      }
      try {
        receiver.receiveSelectResult(this, wantToRead, wantToWrite);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void shutdownOutput() {
    }

    @Override
    public void close() {
      closed = true;
    }

    byte[] writtenBytes() {
      byte[] bytes = new byte[written.position()];
      written.flip().get(bytes);
      return bytes;
    }
  }

  private static final class NoMessagesApplication implements Application {
    @Override
    public boolean forward(RouteMessage message) {
      return true;
    }

    @Override
    public void deliver(rice.p2p.commonapi.Id id, Message message) {
    }

    @Override
    public void update(rice.p2p.commonapi.NodeHandle handle, boolean joined) {
    }
  }

  /** What the connector's application gets: the socket or the exception. */
  static final class Outcome implements AppSocketReceiver {
    final CountDownLatch answered = new CountDownLatch(1);
    volatile AppSocket socket;
    volatile Exception exception;

    @Override
    public void receiveSocket(AppSocket socket) {
      this.socket = socket;
      answered.countDown();
    }

    @Override
    public void receiveSelectResult(AppSocket socket, boolean canRead, boolean canWrite) {
    }

    @Override
    public void receiveException(AppSocket socket, Exception e) {
      this.exception = e;
      answered.countDown();
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    connectorEnv = new Environment();
    node = StubHandleNodes.newNode(env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
    connectorEnv.destroy();
  }

  private static byte[] appIdBytes(int address) {
    return ByteBuffer.allocate(4).putInt(address).array();
  }

  private PastryAppl registeredApplication(String instance) {
    Endpoint endpoint = node.buildEndpoint(new NoMessagesApplication(), instance);
    endpoint.register();
    return (PastryAppl) endpoint;
  }

  @Test
  void theAcceptorAnswersAnUnknownApplicationWithNoApp() throws IOException {
    ScriptedSocket socket = new ScriptedSocket(appIdBytes(0x0BADF00D));

    node.incomingSocket(socket);

    assertEquals(1, socket.writtenBytes().length);
    assertEquals(ANSWER_NO_APP, socket.writtenBytes()[0]);
    assertTrue(socket.closed);
  }

  @Test
  void theAcceptorAnswersAnApplicationThatDoesNotAcceptWithNoAcceptor() throws IOException {
    PastryAppl application = registeredApplication("no-acceptor");
    ScriptedSocket socket = new ScriptedSocket(appIdBytes(application.getAddress()));

    node.incomingSocket(socket);

    assertEquals(ANSWER_NO_ACCEPTOR, socket.writtenBytes()[0]);
    assertFalse(socket.closed);
  }

  @Test
  void theAcceptorHandsTheSocketToTheAcceptingApplication() throws IOException {
    PastryAppl application = registeredApplication("acceptor");
    Outcome outcome = new Outcome();
    application.accept(outcome);
    ScriptedSocket socket = new ScriptedSocket(appIdBytes(application.getAddress()));

    node.incomingSocket(socket);

    assertEquals(ANSWER_OK, socket.writtenBytes()[0]);
    assertNotNull(outcome.socket);
    assertFalse(application.canReceiveSocket());
  }

  /** A node whose transport layer opens the given socket, as {@code connect} does. */
  private PastryNode connectorOver(ScriptedSocket socket) {
    PastryNode connector = new PastryNode(Id.build("1"), connectorEnv);
    @SuppressWarnings("unchecked") // a proxy of the raw interface
    TransportLayer<NodeHandle, RawMessage> transport = (TransportLayer<NodeHandle, RawMessage>) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {TransportLayer.class}, (proxy, method, args) -> {
          if (method.getName().equals("openSocket")) {
            @SuppressWarnings("unchecked")
            SocketCallback<NodeHandle> callback = (SocketCallback<NodeHandle>) args[1];
            SocketRequestHandle<NodeHandle> handle = (SocketRequestHandle<NodeHandle>) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {SocketRequestHandle.class}, (p, m, a) -> null);
            // the connector checks the handle that openSocket returned, so the socket arrives after it returned
            connectorEnv.getSelectorManager().invoke(() -> callback.receiveResult(handle, socket));
            return handle;
          }
          return null;
        });
    @SuppressWarnings("unchecked")
    LivenessProvider<NodeHandle> liveness = (LivenessProvider<NodeHandle>) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {LivenessProvider.class}, (proxy, method, args) -> null);
    @SuppressWarnings("unchecked")
    ProximityProvider<NodeHandle> proximity = (ProximityProvider<NodeHandle>) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {ProximityProvider.class}, (proxy, method, args) -> null);
    connector.setSocketElements(0, 0, transport, liveness, proximity, null);
    return connector;
  }

  private Outcome connectAnswered(byte answer, PastryAppl application) throws InterruptedException {
    ScriptedSocket socket = new ScriptedSocket(answer);
    PastryNode connector = connectorOver(socket);
    Outcome outcome = new Outcome();

    connector.connect(node.getLocalHandle(), outcome, application, 1000);
    assertTrue(outcome.answered.await(10, TimeUnit.SECONDS)); // the connect runs on the selector thread

    assertEquals(4, socket.writtenBytes().length);
    return outcome;
  }

  @Test
  void theConnectorWritesTheApplicationAddressAndHandsOverTheSocketOnOk() throws InterruptedException {
    PastryAppl application = registeredApplication("connector");

    Outcome outcome = connectAnswered(ANSWER_OK, application);

    assertNotNull(outcome.socket);
  }

  @Test
  void theConnectorReportsAnUnknownApplication() throws InterruptedException {
    PastryAppl application = registeredApplication("connector-no-app");

    Outcome outcome = connectAnswered(ANSWER_NO_APP, application);

    assertInstanceOf(AppNotRegisteredException.class, outcome.exception);
  }

  @Test
  void theConnectorReportsAnApplicationThatDoesNotAccept() throws InterruptedException {
    PastryAppl application = registeredApplication("connector-no-acceptor");

    Outcome outcome = connectAnswered(ANSWER_NO_ACCEPTOR, application);

    assertInstanceOf(NoReceiverAvailableException.class, outcome.exception);
  }

  @Test
  void theConnectorReportsAnUnknownAnswer() throws InterruptedException {
    PastryAppl application = registeredApplication("connector-unknown");

    Outcome outcome = connectAnswered((byte) 9, application);

    assertInstanceOf(AppSocketException.class, outcome.exception);
  }
}
