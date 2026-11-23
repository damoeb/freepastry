package rice.p2p.scribe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.lang.reflect.Proxy;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rice.p2p.commonapi.RouteMessage;
import rice.p2p.scribe.ScribeTestSupport.FakeEndpoint;
import rice.p2p.scribe.ScribeTestSupport.Handle;
import rice.p2p.scribe.ScribeTestSupport.Routed;
import rice.p2p.scribe.messaging.AnycastFailureMessage;
import rice.p2p.scribe.messaging.AnycastMessage;

class AnycastHandlerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnycastHandlerTest.class);

  private final Handle local = new Handle(1);
  private final FakeEndpoint fake = new FakeEndpoint(local);
  private final TopicRegistry registry = new TopicRegistry(fake.endpoint, local, ScribeTestSupport.listener(), LOGGER);
  private final ScribePolicy policy = new ScribePolicy.DefaultScribePolicy(null);
  private final AnycastHandler handler = new AnycastHandler(fake.endpoint, local, registry, () -> policy, LOGGER);

  private static RouteMessage routeMessage() {
    return (RouteMessage) Proxy.newProxyInstance(AnycastHandlerTest.class.getClassLoader(), new Class<?>[] { RouteMessage.class },
        (proxy, method, args) -> switch (method.getName()) {
          case "getNextHopHandle" -> null;
          case "toString" -> "RouteMessage";
          default -> throw new UnsupportedOperationException(method.toString());
        });
  }

  @Test
  void anycastThatVisitedTooManyNodesFailsInsteadOfWalkingOn() {
    Topic topic = ScribeTestSupport.topic(1);
    registry.create(topic); // a member without clients: it passes the anycast on
    Handle requestor = new Handle(5);
    AnycastMessage message = new AnycastMessage(requestor, topic, null);
    // the visited list comes from the wire: a forger fills it, and names one more node to visit
    for (int i = 0; i < 300; i++) {
      message.addVisited(new Handle(100 + i));
    }
    message.addLast(new Handle(999));

    assertFalse(handler.forward(routeMessage(), message));

    assertEquals(1, fake.routed.size());
    Routed sent = fake.routed.get(0);
    assertInstanceOf(AnycastFailureMessage.class, sent.message());
    assertEquals(requestor, sent.hint());
  }
}
