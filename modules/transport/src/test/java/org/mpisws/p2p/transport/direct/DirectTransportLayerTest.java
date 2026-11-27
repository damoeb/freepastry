package org.mpisws.p2p.transport.direct;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import rice.environment.Environment;

class DirectTransportLayerTest {

  @SuppressWarnings("unchecked")
  private static DirectTransportLayer<String, String> newLayer(List<String> received, @Nullable IOException socketFailure) {
    ClassLoader loader = DirectTransportLayerTest.class.getClassLoader();
    GenericNetworkSimulator<String, String> simulator = (GenericNetworkSimulator<String, String>) Proxy.newProxyInstance(
        loader, new Class<?>[] {GenericNetworkSimulator.class}, (proxy, method, args) -> null);
    LivenessProvider<String> liveness = (LivenessProvider<String>) Proxy.newProxyInstance(
        loader, new Class<?>[] {LivenessProvider.class}, (proxy, method, args) -> null);
    DirectTransportLayer<String, String> layer =
        new DirectTransportLayer<>("local", simulator, liveness, Environment.directEnvironment());
    layer.setCallback(new TransportLayerCallback<String, String>() {
      @Override
      public void messageReceived(String identifier, String msg, @Nullable Map<String, Object> options) {
        received.add(msg);
      }

      @Override
      public void incomingSocket(P2PSocket<String> socket) throws IOException {
        if (socketFailure != null) {
          throw socketFailure;
        }
      }
    });
    return layer;
  }

  @Test
  void acceptMessagesFalseDropsIncomingMessages() throws IOException {
    List<String> received = new ArrayList<>();
    DirectTransportLayer<String, String> layer = newLayer(received, null);

    layer.incomingMessage("a", "one", null);
    layer.acceptMessages(false);
    layer.incomingMessage("a", "two", null);
    layer.acceptMessages(true);
    layer.incomingMessage("a", "three", null);

    assertEquals(List.of("one", "three"), received);
  }

  @Test
  @SuppressWarnings("unchecked")
  void errorHandlerSeesFailedIncomingSocket() {
    IOException failure = new IOException("boom");
    DirectTransportLayer<String, String> layer = newLayer(new ArrayList<>(), failure);
    List<Throwable> errors = new ArrayList<>();
    layer.setErrorHandler(new ErrorHandler<String>() {
      @Override
      public void receivedUnexpectedData(String identifier, byte[] bytes, int location, @Nullable Map<String, Object> options) {}

      @Override
      public void receivedException(@Nullable String i, Throwable error) {
        errors.add(error);
      }
    });
    P2PSocket<String> socket = (P2PSocket<String>) Proxy.newProxyInstance(
        DirectTransportLayerTest.class.getClassLoader(), new Class<?>[] {P2PSocket.class}, (proxy, method, args) -> "remote");

    layer.finishReceiveSocket(socket);

    assertEquals(List.of(failure), errors);
  }
}
