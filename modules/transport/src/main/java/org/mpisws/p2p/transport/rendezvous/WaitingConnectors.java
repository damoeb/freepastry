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
package org.mpisws.p2p.transport.rendezvous;

import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.P2PSocket;
import org.slf4j.Logger;
import rice.selector.SelectorLoop;
import rice.selector.TimerTask;

/**
 * The connector sockets that wait at a rendezvous node for their acceptor, by requestor, target and uid. A connector
 * that does not find its acceptor within the connector timeout is closed, and at most a configured number of them wait
 * at the same time. Only touched on the selector thread.
 */
class WaitingConnectors<Identifier, HighIdentifier> {
  /** A connector socket at the rendezvous node, and the task that closes it if its acceptor does not come in time. */
  private record WaitingConnector<Identifier>(P2PSocket<Identifier> socket, TimerTask expiry) {}

  /**
   * requestor, target, uid -> connector waiting for its acceptor. Only touched on the selector thread.
   */
  private final Map<HighIdentifier, Map<HighIdentifier, Map<Integer, WaitingConnector<Identifier>>>> connectSockets =
    new HashMap<HighIdentifier, Map<HighIdentifier, Map<Integer, WaitingConnector<Identifier>>>>();
  private int waitingConnectorCount;

  private final Logger logger;
  private final SelectorLoop selectorManager;
  private final int connectorTimeoutMillis;
  private final int maxWaitingConnectors;

  WaitingConnectors(Logger logger, SelectorLoop selectorManager, int connectorTimeoutMillis, int maxWaitingConnectors) {
    this.logger = logger;
    this.selectorManager = selectorManager;
    this.connectorTimeoutMillis = connectorTimeoutMillis;
    this.maxWaitingConnectors = maxWaitingConnectors;
  }

  /**
   * This map stores the connect socket until the corresponding accept socket arrives, for at most
   * {@code transport_rendezvous_connector_timeout_millis}. The socket stays under the handshake deadline of the lower
   * layer while it waits.
   *
   * @return false if the socket was closed instead: too many connectors wait already, or one waits under the same uid
   */
  boolean put(final HighIdentifier requestor, final HighIdentifier target, final int uid, final P2PSocket<Identifier> socket) {
    if (waitingConnectorCount >= maxWaitingConnectors) {
      logger.warn("{} connectors wait for their acceptor already, closing the new one from {} to {} uid:{} {}", waitingConnectorCount, requestor, target, uid, socket);
      socket.close();
      return false;
    }

    Map<HighIdentifier, Map<Integer, WaitingConnector<Identifier>>> socketsByTarget = connectSockets.get(requestor);
    if (socketsByTarget == null) {
      socketsByTarget = new HashMap<HighIdentifier, Map<Integer, WaitingConnector<Identifier>>>();
      connectSockets.put(requestor, socketsByTarget);
    }

    Map<Integer, WaitingConnector<Identifier>> socketsByUid = socketsByTarget.get(target);
    if (socketsByUid == null) {
      socketsByUid = new HashMap<Integer, WaitingConnector<Identifier>>();
      socketsByTarget.put(target, socketsByUid);
    }

    WaitingConnector<Identifier> existing = socketsByUid.get(uid);
    if (existing != null) {
      // error, we have a problem, because there is already a socket registered here with the same uid!!!
      logger.warn("error in storeConnectSocket() there is already a connector with the same UID!!!, dropping the new one.  Old:{} new:{}", existing.socket(), socket);
      socket.close();
      return false;
    }

    TimerTask expiry = new TimerTask() {
      @Override
      public void run() {
        expireConnectSocket(requestor, target, uid, socket);
      }
    };
    socketsByUid.put(uid, new WaitingConnector<>(socket, expiry));
    waitingConnectorCount++;
    if (connectorTimeoutMillis > 0) selectorManager.schedule(expiry, connectorTimeoutMillis);
    return true;
  }

  private void expireConnectSocket(HighIdentifier requestor, HighIdentifier target, int uid, P2PSocket<Identifier> socket) {
    Map<HighIdentifier, Map<Integer, WaitingConnector<Identifier>>> socketsByTarget = connectSockets.get(requestor);
    Map<Integer, WaitingConnector<Identifier>> socketsByUid = socketsByTarget == null ? null : socketsByTarget.get(target);
    WaitingConnector<Identifier> waiting = socketsByUid == null ? null : socketsByUid.get(uid);
    if (waiting == null || !waiting.socket().equals(socket)) return; // its acceptor came, or it was replaced
    logger.info("no acceptor came within {} ms, closing the connector from {} to {} uid:{} {}", connectorTimeoutMillis, requestor, target, uid, socket);
    remove(requestor, target, uid);
    socket.close();
  }

  @Nullable P2PSocket<Identifier> remove(HighIdentifier requestor, HighIdentifier target, int uid) {
    Map<HighIdentifier, Map<Integer, WaitingConnector<Identifier>>> socketsByTarget = connectSockets.get(requestor);
    if (socketsByTarget == null) {
      return null;
    }

    Map<Integer, WaitingConnector<Identifier>> socketsByUid = socketsByTarget.get(target);
    if (socketsByUid == null) {
      return null;
    }

    // clean this up
    WaitingConnector<Identifier> waiting = socketsByUid.remove(uid);
    if (socketsByUid.isEmpty()) socketsByTarget.remove(target);
    if (socketsByTarget.isEmpty()) connectSockets.remove(requestor);
    if (waiting == null) return null;

    waiting.expiry().cancel();
    waitingConnectorCount--;
    return waiting.socket();
  }

  /** Closes every connector that still waits for its acceptor. */
  void closeAll() {
    for (Map<HighIdentifier, Map<Integer, WaitingConnector<Identifier>>> socketsByTarget : connectSockets.values()) {
      for (Map<Integer, WaitingConnector<Identifier>> socketsByUid : socketsByTarget.values()) {
        for (WaitingConnector<Identifier> waiting : socketsByUid.values()) {
          waiting.expiry().cancel();
          waiting.socket().close();
        }
      }
    }
    connectSockets.clear();
    waitingConnectorCount = 0;
  }
}
