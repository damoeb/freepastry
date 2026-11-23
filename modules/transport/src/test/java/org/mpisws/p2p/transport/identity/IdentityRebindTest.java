package org.mpisws.p2p.transport.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;
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
 * {@link BindStrategy.Replacement#REBIND}: when a bind strategy finds neither claim on an address proven, the
 * binding follows the new one and nobody is marked dead forever.
 */
class IdentityRebindTest {

  private Environment env;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  /** Upper identifiers are "id@address"; the middle and lower identifiers are the address. */
  private static class AtAddressSerializer implements IdentitySerializer<String, String, String> {
    @Override
    public void serialize(OutputBuffer buf, String identifier) throws IOException {}

    @Override
    public String deserialize(InputBuffer buf, String lowerIdentifier) throws IOException {
      throw new IOException("not used");
    }

    @Override
    public String translateDown(String identifier) {
      return identifier.substring(identifier.indexOf('@') + 1);
    }

    @Override
    public String translateUp(String identifier) {
      return identifier;
    }

    @Override
    public void addSerializerListener(SerializerListener<String> listener) {}

    @Override
    public void removeSerializerListener(SerializerListener<String> listener) {}
  }

  private static class Rebinds implements BindStrategy<String, String> {
    @Override
    public boolean accept(String upperIdentifier, @Nullable String lowerIdentifier, @Nullable Map<String, Object> options) {
      return true;
    }

    @Override
    public Replacement replace(String old, String upperIdentifier, @Nullable Map<String, Object> options) {
      return Replacement.REBIND;
    }
  }

  @Test
  @DisplayName("a rebinding moves the address to the new identifier and marks neither one dead forever")
  void rebindMarksNoOneDead() throws Exception {
    // the node change rule would always pick the old one, and mark the new one dead forever
    IdentityLayerCore<String, String, String, String> identity =
        new IdentityLayerCore<>(new byte[0], new AtAddressSerializer(), (oldDest, newDest) -> false, (upper, middle) -> true, new Rebinds(), env);
    identity.setOverrideLiveness((lower, liveness, options) -> {});

    CompletableFuture<Boolean> bound = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> {
      try {
        identity.addBinding("honest@A", "A", null);
        bound.complete(identity.addBinding("claimant@A", "A", null) && identity.addBinding("honest@A", "A", null));
      } catch (Throwable throwable) {
        bound.completeExceptionally(throwable);
      }
    });

    assertTrue(bound.get(10, TimeUnit.SECONDS), "both bindings accepted");
    assertEquals("honest@A", identity.bindings.get("A"));
    assertTrue(identity.deadForever.isEmpty(), "dead forever: " + identity.deadForever);
  }
}
