package rice.pastry.standard;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.GenericSimulatorListener;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.join.JoinRequest;

import static org.junit.jupiter.api.Assertions.assertSame;

class ConsistentJoinProtocolJoinAnswerTest {

  private static final long TIMEOUT_SECONDS = 10;

  private Environment env;
  private NetworkSimulator<DirectNodeHandle, RawMessage> simulator;
  private PastryNodeFactory factory;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator, env);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  @DisplayName("the root answers a joiner with its own leaf set")
  void theRootAnswersAJoinerWithItsOwnLeafSet() throws Exception {
    PastryNode root = factory.newNode();
    PastryNode joiner = factory.newNode();
    CompletableFuture<JoinRequest> answer = new CompletableFuture<JoinRequest>();
    simulator.addSimulatorListener(new GenericSimulatorListener<DirectNodeHandle, RawMessage>() {
      @Override
      public void messageSent(RawMessage message, DirectNodeHandle from, DirectNodeHandle receiver, int delay) {
        if (message instanceof JoinRequest joinRequest && joinRequest.accepted()) {
          answer.complete(joinRequest);
        }
      }

      @Override
      public void messageReceived(RawMessage message, DirectNodeHandle from, DirectNodeHandle receiver) {}
    });

    root.boot((Object) null);
    joiner.boot(root.getLocalHandle());

    assertSame(root.getLeafSet(), answer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getLeafSet());
  }
}
