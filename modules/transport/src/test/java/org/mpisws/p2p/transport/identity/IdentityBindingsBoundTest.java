package org.mpisws.p2p.transport.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * Every datagram's identity header offers a binding of an address to a node handle, so a remote sender can offer as
 * many as it likes. The table must stay bounded, and must not forget the bindings that protect authenticated nodes.
 */
class IdentityBindingsBoundTest {
  private static final int MAX_BINDINGS = 100;
  private static final int SPOOFED_IDENTITIES = 10 * MAX_BINDINGS;

  private Environment env;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    env.getParameters().setInt("pastry_socket_identity_max_bindings", MAX_BINDINGS);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  /** Upper, middle and lower identifiers are all the same string. */
  private static class StubSerializer implements IdentitySerializer<String, String, String> {
    @Override
    public void serialize(OutputBuffer buf, String i) throws IOException {}

    @Override
    public String deserialize(InputBuffer buf, String l) throws IOException {
      return l;
    }

    @Override
    public String translateDown(String i) {
      return i;
    }

    @Override
    public String translateUp(String i) {
      return i;
    }

    @Override
    public void addSerializerListener(SerializerListener<String> listener) {}

    @Override
    public void removeSerializerListener(SerializerListener<String> listener) {}
  }

  private IdentityImpl<String, String, String, String> identity(@Nullable BindStrategy<String, String> strategy) {
    IdentityImpl<String, String, String, String> identity =
        new IdentityImpl<>(new byte[0], new StubSerializer(), (oldDest, newDest) -> false, (upper, middle) -> true, strategy, env);
    identity.setOverrideLiveness((l, liveness, options) -> {});
    return identity;
  }

  private void onSelector(Runnable action) throws Exception {
    CompletableFuture<Void> done = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> {
      try {
        action.run();
        done.complete(null);
      } catch (Throwable t) {
        done.completeExceptionally(t);
      }
    });
    done.get(10, TimeUnit.SECONDS);
  }

  private static void spoof(IdentityImpl<String, String, String, String> identity) {
    for (int k = 0; k < SPOOFED_IDENTITIES; k++) {
      identity.addBinding("spoofed" + k, "spoofed" + k, null);
    }
  }

  @Test
  @DisplayName("bindings offered by spoofed identity headers do not grow the table beyond its cap")
  void spoofedIdentitiesAreBounded() throws Exception {
    var identity = identity(null);

    onSelector(() -> spoof(identity));

    assertTrue(identity.bindings.size() <= MAX_BINDINGS, "bindings: " + identity.bindings.size());
  }

  @Test
  @DisplayName("a binding with messages waiting for it survives a flood of spoofed identities")
  void bindingInUseSurvivesTheFlood() throws Exception {
    var identity = identity(null);

    onSelector(() -> {
      identity.addBinding("busy", "busy", null);
      identity.addPendingMessage("busy", identity.new IdentityMessageHandle("busy", "msg", null, null));
      spoof(identity);
    });

    assertEquals("busy", identity.bindings.get("busy"));
  }
}
