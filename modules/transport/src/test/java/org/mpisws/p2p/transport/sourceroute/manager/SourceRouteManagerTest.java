package org.mpisws.p2p.transport.sourceroute.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import rice.environment.params.simple.SimpleParameters;

class SourceRouteManagerTest {

  @Test
  void limitKeepsAtMostTheConfiguredNumberOfRoutes() {
    assertEquals(List.of(1, 2), List.copyOf(SourceRouteManager.limit(List.of(1, 2, 3, 4), 2)));
    assertEquals(List.of(1, 2), List.copyOf(SourceRouteManager.limit(List.of(1, 2), 8)));
  }

  @Test
  void zeroAttemptsDisablesSourceRouting() {
    assertEquals(0, SourceRouteManager.limit(List.of(1, 2, 3), 0).size());
  }

  @Test
  void configReadsTheAttemptsAndRejectsNegatives() {
    var p = new SimpleParameters(new String[] {}, null);
    p.setInt("pastry_socket_srm_num_source_route_attempts", 3);
    assertEquals(3, SourceRouteManagerConfig.fromParameters(p).numSourceRouteAttempts());
    p.setInt("pastry_socket_srm_num_source_route_attempts", -1);
    assertThrows(IllegalArgumentException.class, () -> SourceRouteManagerConfig.fromParameters(p));
  }
}
