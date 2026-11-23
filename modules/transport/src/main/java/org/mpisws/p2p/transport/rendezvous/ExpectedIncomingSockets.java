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
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.slf4j.Logger;
import rice.p2p.util.tuples.Tuple;

/**
 * The sockets that the local node asked for through a rendezvous node and that it still waits for: when the acceptor
 * socket arrives at the local node, it is delivered to whoever asked, by the contact it was asked for and the uid of
 * the request. Only touched on the selector thread.
 */
class ExpectedIncomingSockets<Identifier, HighIdentifier> {
  private final Logger logger;

  ExpectedIncomingSockets(Logger logger) {
    this.logger = logger;
  }

  private final Map<HighIdentifier, Map<Integer, Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>>> expectedIncomingSockets =
    new HashMap<HighIdentifier, Map<Integer, Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>>>();

  void put(HighIdentifier contact, int uid,
      SocketCallback<Identifier> deliverSocketToMe, SocketRequestHandle<Identifier> requestHandle) {
    logger.atTrace().log(() -> "putExpectedIncomingSocket("+contact+"@"+System.identityHashCode(contact)+","+uid+","+deliverSocketToMe+","+requestHandle+")");
    Map<Integer, Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>> socketsByUid = expectedIncomingSockets.get(contact);
    if (socketsByUid == null) {
      socketsByUid = new HashMap<Integer, Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>>();
      expectedIncomingSockets.put(contact, socketsByUid);
    }

    if (socketsByUid.containsKey(uid)) {
      throw new IllegalStateException("putExpectedIncomingSockets("+contact+","+uid+","+deliverSocketToMe+") already contains "+socketsByUid.get(uid));
    }

    socketsByUid.put(uid, new Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>(deliverSocketToMe, requestHandle));
  }

  @Nullable Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>> remove(HighIdentifier target, int uid) {
    logger.atTrace().log(() -> "removeExpectedIncomingSocket("+target+"@"+System.identityHashCode(target)+","+uid+")");

    Map<Integer, Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>>> socketsByUid = expectedIncomingSockets.get(target);
    if (socketsByUid == null) {
      if (logger.isTraceEnabled()) {
        String text = "";
        for (HighIdentifier highIdentifier : expectedIncomingSockets.keySet()) {
          text+=" "+highIdentifier;
        }
        logger.trace("removeExpectedIncomingSocket("+target+"@"+System.identityHashCode(target)+","+uid+") had no first level entry for target"+text);
      }
      return null;
    }
    Tuple<SocketCallback<Identifier>, SocketRequestHandle<Identifier>> ret = socketsByUid.get(uid);

    if (ret == null) {
      if (logger.isTraceEnabled()) {
          String s = "";
        for (Integer h : socketsByUid.keySet()) {
          s+=" "+h;
        }
        logger.trace("removeExpectedIncomingSocket("+target+"@"+System.identityHashCode(target)+","+uid+") had no uid for target"+s);
      }
    } else {
      socketsByUid.remove(uid);
    }
    if (socketsByUid.isEmpty()) expectedIncomingSockets.remove(target);

    return ret;
  }
}
