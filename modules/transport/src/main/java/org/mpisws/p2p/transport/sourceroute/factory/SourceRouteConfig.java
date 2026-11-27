package org.mpisws.p2p.transport.sourceroute.factory;

import static rice.environment.params.ConfigChecks.requirePositive;

import rice.environment.params.Parameters;

/**
 * The tuning parameters of the source route factories.
 *
 * @param maxNumHops {@code transport_sr_max_num_hops}: the most hops of a source route read from the
 *        wire; a peer sending a longer one is rejected
 */
public record SourceRouteConfig(int maxNumHops) {

  /** The value of {@code transport_sr_max_num_hops} in the default parameters. */
  public static final int DEFAULT_MAX_NUM_HOPS = 5;

  /**
   * @throws IllegalArgumentException if a value is not positive
   */
  public static SourceRouteConfig fromParameters(Parameters parameters) {
    return new SourceRouteConfig(requirePositive("transport_sr_max_num_hops", parameters.getInt("transport_sr_max_num_hops")));
  }
}
