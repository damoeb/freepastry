package rice.pastry.routing;

import org.junit.jupiter.api.Test;
import rice.pastry.Id;
import rice.pastry.testing.LeafSetTest.TestNodeHandle;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class RouteSetTest {

  private static final int MAX_SIZE = 3;

  private final TestNodeHandle member = new TestNodeHandle(Id.build("1234"));
  private final RouteSet routeSet = new RouteSet(MAX_SIZE, 0, 0, null, member);

  @Test
  void getHandleByCommonApiIdFindsMember() {
    rice.p2p.commonapi.Id id = member.getNodeId();

    assertSame(member, routeSet.getHandle(id));
  }

  @Test
  void getHandleByCommonApiIdReturnsNullForNonMember() {
    rice.p2p.commonapi.Id id = Id.build("5678");

    assertNull(routeSet.getHandle(id));
  }
}
