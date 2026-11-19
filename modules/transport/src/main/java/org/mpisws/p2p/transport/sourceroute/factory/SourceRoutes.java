package org.mpisws.p2p.transport.sourceroute.factory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

final class SourceRoutes {
  private SourceRoutes() {}

  /**
   * Forwarding finds the local node with {@code getHop(local)}, which returns the first match. A route that names a
   * node twice therefore makes that node forward to the wrong successor, and a spoofed route such as [X, A, B, A, Z]
   * or [X, A, A, Z] bounces between (or back to) the same nodes forever. Routes built by this stack never repeat a
   * hop (direct [A, B], relayed [A, R, B], and their reversals), so a repeat can only come from the network and is
   * rejected while the route is read.
   */
  static <T> void requireDistinctHops(List<T> path) throws IOException {
    if (new HashSet<>(path).size() != path.size()) {
      throw new IOException("Source route repeats a hop: " + path);
    }
  }
}
