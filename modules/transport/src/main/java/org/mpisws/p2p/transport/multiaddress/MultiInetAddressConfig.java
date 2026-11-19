package org.mpisws.p2p.transport.multiaddress;

import static rice.environment.params.ConfigChecks.requirePositive;

import rice.environment.params.Parameters;

/**
 * The tuning parameters of a {@link MultiInetAddressTransportLayer}.
 *
 * @param maxNumAddresses {@code transport_epoch_max_num_addresses}: the most addresses a remote node may
 *        announce for itself; a peer announcing more is rejected
 */
public record MultiInetAddressConfig(int maxNumAddresses) {

  /**
   * @throws IllegalArgumentException if a value is not positive
   */
  public static MultiInetAddressConfig fromParameters(Parameters parameters) {
    return new MultiInetAddressConfig(
      requirePositive("transport_epoch_max_num_addresses", parameters.getInt("transport_epoch_max_num_addresses")));
  }
}
