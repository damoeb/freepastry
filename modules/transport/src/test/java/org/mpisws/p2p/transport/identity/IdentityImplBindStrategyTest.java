package org.mpisws.p2p.transport.identity;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

class IdentityImplBindStrategyTest {

  private Environment env;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

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

  private IdentityImpl<String, String, String, String> identity(BindStrategy<String, String> strategy) {
    return new IdentityImpl<>(new byte[0], new StubSerializer(), (oldDest, newDest) -> false, (upper, middle) -> true, strategy, env);
  }

  private boolean addBindingOnSelector(IdentityImpl<String, String, String, String> identity) throws Exception {
    CompletableFuture<Boolean> result = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> {
      try {
        result.complete(identity.addBinding("peer", null, null));
      } catch (Throwable t) {
        result.completeExceptionally(t);
      }
    });
    return result.get(10, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("a binding from the wire (no options) is rejected by a rejecting bind strategy")
  void wirePathBindingIsRejected() throws Exception {
    AtomicInteger asked = new AtomicInteger();
    var identity = identity((u, l, options) -> {
      asked.incrementAndGet();
      return false;
    });

    assertFalse(addBindingOnSelector(identity));
    assertTrue(asked.get() == 1, "the bind strategy was consulted");
  }
}
