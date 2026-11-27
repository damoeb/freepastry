package rice.pastry.standard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeListener;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.GenericSimulatorListener;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.join.JoinRequest;
import rice.pastry.leafset.BroadcastLeafSet;
import rice.pastry.leafset.RequestLeafSet;
import rice.pastry.messaging.PastryRawMessage;
import rice.pastry.routing.BroadcastRouteRow;
import rice.pastry.routing.RequestRouteRow;
import rice.selector.TimerTask;

/**
 * R3 (design 3f): one peer floods a node with maintenance requests it can send at will. Without budgets the victim
 * answers every one; with them it answers the peer's burst and ignores the rest. Runs in the direct simulator, whose
 * clock is simulated, so the flood arrives at one instant and no bucket refills during it.
 */
class MaintenanceFloodTest {
  private static final long TIMEOUT_SECONDS = 20;
  private static final int FLOOD = 100;
  private static final long SETTLE_MILLIS = 10_000;

  private Environment env;
  private NetworkSimulator<DirectNodeHandle, RawMessage> simulator;
  private PastryNode victim;
  private PastryNode attacker;
  /** messages the victim sent to the attacker, by class */
  private final Map<Class<?>, Integer> answers = new ConcurrentHashMap<>();

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    simulator = new EuclideanNetwork<>(env);
    DirectPastryNodeFactory factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator, env);
    victim = factory.newNode();
    attacker = factory.newNode();
    CompletableFuture<Void> isReady = new CompletableFuture<>();
    victim.addPastryNodeListener(new PastryNodeListener() {
      @Override
      public void pastryNodeReady(PastryNode node, boolean ready) {
        if (ready) isReady.complete(null);
      }
    });
    victim.boot((Object) null);
    isReady.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    simulator.addSimulatorListener(new GenericSimulatorListener<>() {
      @Override
      public void messageSent(RawMessage message, DirectNodeHandle from, DirectNodeHandle receiver, int delay) {
        if (from.equals(victim.getLocalHandle()) && receiver.equals(attacker.getLocalHandle())) {
          answers.merge(message.getClass(), 1, Integer::sum);
        }
      }

      @Override
      public void messageReceived(RawMessage message, DirectNodeHandle from, DirectNodeHandle receiver) {}
    });
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  /** The attacker sends {@link #FLOOD} messages at one simulated instant; then the simulation runs on until all is answered. */
  private void flood(Function<NodeHandle, PastryRawMessage> message) throws Exception {
    CompletableFuture<Void> settled = new CompletableFuture<>();
    env.getSelectorManager().invoke(() -> {
      for (int n = 0; n < FLOOD; n++) {
        attacker.send(victim.getLocalHandle(), message.apply(attacker.getLocalHandle()), null, null);
      }
      env.getSelectorManager().getTimer().schedule(new TimerTask() {
        @Override
        public void run() {
          settled.complete(null);
        }
      }, SETTLE_MILLIS);
    });
    settled.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private int answers(Class<?> type) {
    return answers.getOrDefault(type, 0);
  }

  @Test
  @DisplayName("R3: a peer that floods route row requests gets its burst answered, not every request")
  void routeRowRequestFlood() throws Exception {
    flood(sender -> new RequestRouteRow(sender, (short) 0));

    int answered = answers(BroadcastRouteRow.class);
    assertTrue(answered >= 1, "the first requests are answered");
    assertTrue(answered <= 5, "answered " + answered + " of " + FLOOD + " requests");
  }

  @Test
  @DisplayName("R3: a peer that floods leaf set requests gets its burst answered, not every request")
  void leafSetRequestFlood() throws Exception {
    flood(sender -> new RequestLeafSet(sender, 0));

    int answered = answers(BroadcastLeafSet.class);
    assertTrue(answered >= 1, "the first requests are answered");
    assertTrue(answered <= 5, "answered " + answered + " of " + FLOOD + " requests");
  }

  @Test
  @DisplayName("R3: a joiner that floods join requests at the root gets two answers per minute, not one per request")
  void joinRequestFlood() throws Exception {
    flood(sender -> new JoinRequest(sender, victim.getRoutingTable().baseBitLength(), 1L));

    assertEquals(2, answers(JoinRequest.class));
  }
}
