/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is"
basis, without any representations or warranties of any kind, express or implied
including, but not limited to, representations or warranties of
non-infringement, merchantability or fitness for a particular purpose. In no
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use, data, or
profits; or business interruption) however caused and on any theory of
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if
advised of the possibility of such damage.

*******************************************************************************/
package org.mpisws.p2p.transport.identity;

import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Decides which upper identifiers {@link IdentityImpl} binds to a lower one, and which received messages it passes
 * up as coming from them.
 */
public interface BindStrategy<UpperIdentifier, LowerIdentifier> {
  /**
   * @param u the upper identifier offered for the lower one
   * @param l the lower identifier, or null when the upper one was learned without a connection (for example
   *     deserialized from a message)
   * @param options the options of the connection or message it came with, or null
   * @return false to refuse the binding
   */
  public boolean accept(UpperIdentifier u, @Nullable LowerIdentifier l, @Nullable Map<String, Object> options);

  /**
   * Whether a received message may go up to the layers above with {@code from} as its sender. The identity layer
   * asks this for every message, whatever channel it came over: a message read from a socket carries that
   * socket's options, a datagram the options of the datagram.
   *
   * @param from the sender the identity layer read from the message's channel or header
   * @param options the options the message arrived with
   * @return false to drop the message; true by default
   */
  default boolean acceptsMessage(UpperIdentifier from, @Nullable Map<String, Object> options) {
    return true;
  }

  /** What {@link IdentityImpl} does when a lower identifier that is bound to one upper identifier is offered another. */
  enum Replacement {
    /** the new one replaces the old one, which is marked dead for good */
    REPLACE,
    /** the old one stays bound, the new one is refused (and not marked dead) */
    KEEP,
    /** the {@link NodeChangeStrategy} decides; the one it rejects is marked dead for good */
    ASK_NODE_CHANGE_STRATEGY
  }

  /**
   * Decides whether {@code u} may replace {@code old} as the upper identifier of the same lower identifier. It is
   * asked after {@link #accept} has accepted {@code u} with the same options.
   *
   * @param old the upper identifier bound now
   * @param u the upper identifier offered instead
   * @param options the options of the connection or message {@code u} came with, or null
   * @return {@link Replacement#ASK_NODE_CHANGE_STRATEGY} by default
   */
  default Replacement replace(UpperIdentifier old, UpperIdentifier u, @Nullable Map<String, Object> options) {
    return Replacement.ASK_NODE_CHANGE_STRATEGY;
  }
}
