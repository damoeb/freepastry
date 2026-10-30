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
package org.mpisws.p2p.transport.nat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.environment.time.TimeSource;

/**
 * Drops all incoming TCP connections.
 * Drops all incoming UDP connections that we didn't initiate, or that are since UDP_OPEN_MILLIS
 *
 * @author Jeff Hoye
 */
public class FirewallTLImpl<Identifier, MessageType> implements TransportLayer<Identifier, MessageType>, TransportLayerCallback<Identifier, MessageType> {

  private final int UDP_OPEN_MILLIS;

  /**
   * Holds when we last refreshed the UDP connection
   */
  private final Map<Identifier, Long> udpTable;

  private final TransportLayer<Identifier, MessageType> tl;

  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  private TransportLayerCallback<Identifier, MessageType> callback;

  private final TimeSource timeSource;

  private final Environment environment;

  private final Logger logger;

  /**
   *
   * @param udpOpenMillis how long the udp hole remains open
   */
  public FirewallTLImpl(TransportLayer<Identifier, MessageType> tl, int udpOpenMillis, Environment env) {
    this.UDP_OPEN_MILLIS = udpOpenMillis;
    this.environment = env;
    this.timeSource = environment.getTimeSource();
    this.logger = env.getLogManager().getLogger(FirewallTLImpl.class, null);
    this.udpTable = new HashMap<Identifier, Long>();
    this.tl = tl;
    tl.setCallback(this);
    tl.acceptSockets(false);
  }

  @Override
  public @Nullable MessageRequestHandle<Identifier, MessageType> sendMessage(Identifier i, MessageType m, @Nullable MessageCallback<Identifier, MessageType> deliverAckToMe, @Nullable Map<String, Object> options) {
    long now = timeSource.currentTimeMillis();
    udpTable.put(i,now);
    return tl.sendMessage(i, m, deliverAckToMe, options);
  }

  @Override
  public void messageReceived(Identifier i, MessageType m, @Nullable Map<String, Object> options) throws IOException {
    if (udpTable.containsKey(i)) {
      long now = timeSource.currentTimeMillis();
      if (udpTable.get(i)+UDP_OPEN_MILLIS >= now) {
        logger.trace("accepting messageReceived({},{},{})", i, m, options);
        udpTable.put(i,now);
        callback.messageReceived(i, m, options);
        return;
      }
    }
    logger.debug("dropping messageReceived({},{},{})", i, m, options);
  }

  /**
   * Only allow outgoing sockets.
   */
  @Override
  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
    logger.debug("closing incomingSocket({})", s);
    s.close();
  }

  @Override
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  @Override
  public void acceptSockets(boolean b) {
  }

  @Override
  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  @Override
  public @Nullable SocketRequestHandle<Identifier> openSocket(Identifier i, SocketCallback<Identifier> deliverSocketToMe, @Nullable Map<String, Object> options) {
    return tl.openSocket(i, deliverSocketToMe, options);
  }

  @Override
  public void setCallback(TransportLayerCallback<Identifier, MessageType> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
  }

  @Override
  public void destroy() {
    tl.destroy();
  }
}
