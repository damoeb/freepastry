package rice.pastry.standard;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.PastryNode;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.transport.BootstrapAddressNodeHandle;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionHandlerTest {

  private Environment env;
  private PastryNode local;

  @BeforeEach
  void setUp() throws Exception {
    env = Environment.directEnvironment(1);
    NetworkSimulator<DirectNodeHandle, RawMessage> simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
    local = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator, env).newNode();
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void rejoinThroughABogusHandleBootsViaItsAddress() {
    List<Collection<InetSocketAddress>> booted = new ArrayList<>();
    Bootstrapper<InetSocketAddress> recorder = booted::add;
    local.setJoinProtocols(recorder, null, null, null);
    InetSocketAddress address = InetSocketAddress.createUnresolved("bootstrap.example", 9001);
    PartitionHealer handler = new PartitionHealer(local, null, new InetSocketAddress[0]);

    handler.rejoin(new BootstrapAddressNodeHandle(address));

    assertEquals(List.of(List.of(address)), booted);
  }
}
