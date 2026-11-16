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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.proximity.ProximityListener;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import org.slf4j.Logger;
import rice.environment.Environment;

import static org.mpisws.p2p.transport.identity.IdentityImpl.NODE_HANDLE_FROM_INDEX;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD_FOREVER;

class UpperIdentityImpl<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier> implements
    UpperIdentity<UpperIdentifier, UpperMsgType>,
    TransportLayerCallback<MiddleIdentifier, UpperMsgType>,
    LivenessListener<MiddleIdentifier>,
    ProximityListener<MiddleIdentifier> {
  private final IdentityImpl<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier> identity;
  private final Environment environment;
  private final IdentitySerializer<UpperIdentifier, MiddleIdentifier, LowerIdentifier> serializer;
  private final Set<UpperIdentifier> deadForever;
  private final SanityChecker<UpperIdentifier, MiddleIdentifier> sanityChecker;
  private final BindStrategy<UpperIdentifier, LowerIdentifier> bindStrategy;

  TransportLayer<MiddleIdentifier, UpperMsgType> tl;
  ProximityProvider<MiddleIdentifier> prox;

  @SuppressWarnings("NullAway.Init") // set by setErrorHandler() before the layer is used
  private ErrorHandler<UpperIdentifier> errorHandler;
  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  private TransportLayerCallback<UpperIdentifier, UpperMsgType> callback;
  Logger logger;
  private LivenessProvider<MiddleIdentifier> livenessProvider;
  private UpperIdentifier localIdentifier;

  UpperIdentityImpl(
      IdentityImpl<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier> identity,
      UpperIdentifier local,
      TransportLayer<MiddleIdentifier, UpperMsgType> tl,
      LivenessProvider<MiddleIdentifier> live,
      ProximityProvider<MiddleIdentifier> prox) {
    this.identity = identity;
    this.environment = identity.environment;
    this.serializer = identity.serializer;
    this.deadForever = identity.deadForever;
    this.sanityChecker = identity.sanityChecker;
    this.bindStrategy = identity.bindStrategy;
    this.localIdentifier = local;
    this.tl = tl;
    this.livenessProvider = live;
    this.prox = prox;
    logger = environment.getLogManager().getLogger(IdentityImpl.class, "upper");

    tl.setCallback(this);
    livenessProvider.addLivenessListener(this);
    prox.addProximityListener(this);
  }

  @Override
  public void clearState(UpperIdentifier i) {
    livenessProvider.clearState(serializer.translateDown(i));
  }

  @Override
  public SocketRequestHandle<UpperIdentifier> openSocket(
      final UpperIdentifier i,
      final SocketCallback<UpperIdentifier> deliverSocketToMe,
      final @Nullable Map<String, Object> options) {
    logger.debug("openSocket({},{},{})", i, deliverSocketToMe, options);

    final SocketRequestHandleImpl<UpperIdentifier> handle =
      new SocketRequestHandleImpl<UpperIdentifier>(i, options, logger);

    MiddleIdentifier middle = serializer.translateDown(i);

    if (identity.addBinding(i, null, options)) {
      // no problem, sending message
    } else {
      deliverSocketToMe.receiveException(handle, new NodeIsFaultyException(i));
      return handle;
    }

    @Nullable Map<String, Object> newOptions = OptionsFactory.copyOptions(options);
    newOptions.put(NODE_HANDLE_FROM_INDEX, i);


    handle.setSubCancellable(tl.openSocket(middle, new SocketCallback<MiddleIdentifier>(){
      @Override
      public void receiveException(SocketRequestHandle<MiddleIdentifier> s, Exception ex) {
        deliverSocketToMe.receiveException(handle, ex);
      }
      @Override
      public void receiveResult(SocketRequestHandle<MiddleIdentifier> cancellable, P2PSocket<MiddleIdentifier> sock) {
        deliverSocketToMe.receiveResult(handle, new SocketWrapperSocket<UpperIdentifier, MiddleIdentifier>(i,sock,logger,errorHandler,options));
      }
    }, newOptions));
    return handle;
  }

  /**
   * Synchronization by selector thread.  Make sure setDeadForever() is only called on selector, otherwise the
   * next layer down could get stuck trying to send to dead-forever node.
   */
  @Override
  public MessageRequestHandle<UpperIdentifier, UpperMsgType> sendMessage(
      UpperIdentifier i,
      UpperMsgType m,
      @Nullable MessageCallback<UpperIdentifier, UpperMsgType> deliverAckToMe,
      @Nullable Map<String, Object> options) {
    if (!environment.getSelectorManager().isSelectorThread()) throw new RuntimeException("Must be called on selector thread.");
    logger.debug("sendMessage({},{},{})", i, m, options);

    // how to synchronized this properly?  It's too bad that we have to hold the lock for the calls into the lower levels.
    // an alternative would be to re-synchronized and check again, and immediately cancel the message
    if (deadForever.contains(i)) {
      MessageRequestHandle<UpperIdentifier, UpperMsgType> mrh =
        new MessageRequestHandleImpl<UpperIdentifier, UpperMsgType>(i, m, options);
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(mrh, new NodeIsFaultyException(i, m));
      return mrh;
    }

    IdentityImpl<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier>.IdentityMessageHandle ret;

    MiddleIdentifier middle = serializer.translateDown(i);
    if (identity.addBinding(i, null, options)) {
      // no problem, sending message
    } else {
      MessageRequestHandle<UpperIdentifier, UpperMsgType> mrh =
        new MessageRequestHandleImpl<UpperIdentifier, UpperMsgType>(i, m, options);
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(mrh, new NodeIsFaultyException(i, m));
      return mrh;
    }

      options = OptionsFactory.copyOptions(options);
      options.put(NODE_HANDLE_FROM_INDEX, i);
      ret = identity.new IdentityMessageHandle(i, m, options, deliverAckToMe);
      identity.addPendingMessage(i, ret);

    // the synchronization problem is here, if it goes dead now, then this message won't
    // be cancelled... TODO: get this correct, it's probably currently broken, maybe
    // need cancelled bit in ret so that it knows it's cancelled.
    ret.setSubCancellable(tl.sendMessage(middle, m, ret, options));
    return ret;
  }

  @Override
  public void incomingSocket(P2PSocket<MiddleIdentifier> s) throws IOException {
    logger.debug("incomingSocket({})", s);
    final UpperIdentifier from = identity.getIntendedDest(s.getOptions());

    if (from == null) {
      errorHandler.receivedException(null, new MemoryExpiredException("No record of the upper identifier for "+s.getIdentifier()+" index="+IdentityImpl.index(s.getOptions())));
      s.close();
      return;
    }


    if (sanityChecker.isSane(from, s.getIdentifier())) {
      callback.incomingSocket(new SocketWrapperSocket<UpperIdentifier, MiddleIdentifier>(from, s, logger, errorHandler, s.getOptions()));
    } else {
      logger.warn("incomingSocket() Sanity checker did not match {} to {} options:{}", from, s.getIdentifier(), s.getOptions(), new Exception("Stack Trace"));
      s.close();
    }
  }

  @Override
  public void messageReceived(MiddleIdentifier i, UpperMsgType m, @Nullable Map<String, Object> options) throws IOException {
    logger.debug("messageReceived({},{},{})", i, m, options);
    final UpperIdentifier from = identity.getIntendedDest(options);

    if (from == null) {
      errorHandler.receivedException(null, new MemoryExpiredException("No record of the upper identifier for "+i+" index="+IdentityImpl.index(options)+" dropping message"+m));
      return;
    }

    if (bindStrategy != null && !bindStrategy.acceptsMessage(from, options)) {
      logger.info("messageReceived() dropping a message from {} via {}: the bind strategy does not accept it from that channel", from, i);
      return;
    }

    if (sanityChecker.isSane(from, i)) {
      callback.messageReceived(from, m, options);
    } else {
      logger.warn("messageReceived() Sanity checker did not match {} to {} options:{}", from, i, options, new Exception("Stack Trace"));
    }
  }


  @Override
  public boolean checkLiveness(UpperIdentifier i, @Nullable Map<String, Object> options) {
    logger.debug("checkLiveness({},{})", i, options);
    if (deadForever.contains(i)) return false;

    options = OptionsFactory.copyOptions(options);
    options.put(NODE_HANDLE_FROM_INDEX, i);
    return livenessProvider.checkLiveness(serializer.translateDown(i), options);
  }

  final List<LivenessListener<UpperIdentifier>> livenessListeners = new CopyOnWriteArrayList<LivenessListener<UpperIdentifier>>();
  @Override
  public void addLivenessListener(LivenessListener<UpperIdentifier> name) {
    livenessListeners.add(name);
  }

  @Override
  public boolean removeLivenessListener(LivenessListener<UpperIdentifier> name) {
    return livenessListeners.remove(name);
  }

  @Override
  public int getLiveness(UpperIdentifier i, @Nullable Map<String, Object> options) {
    logger.trace("getLiveness({},{})", i, options);
    if (deadForever.contains(i)) return LIVENESS_DEAD_FOREVER;
    options = OptionsFactory.copyOptions(options);
    options.put(NODE_HANDLE_FROM_INDEX, i);

    return livenessProvider.getLiveness(serializer.translateDown(i), options);
  }

  @Override
  public void livenessChanged(MiddleIdentifier i, int val, @Nullable Map<String, Object> options) {
    if (deadForever.contains(i)) {
      if (val < LIVENESS_DEAD) {
        logger.error("Node {} came back from the dead!  It's a miracle! {} Ignoring.", i, val);
      }
      return;
    }

    UpperIdentifier upper = identity.getIntendedDest(options);
      if (upper == null) {
        logger.atWarn().setCause(new Exception("Stack Trace")).log(() -> "Memory for index "+IdentityImpl.index(options)+" collected suppressing livenessChanged()");
        return;
      }
    setLiveness(upper, val, options);
  }

  /**
   * This is a guard so we don't notify about changes in liveness too many times.
   */
  protected void setLiveness(UpperIdentifier i, int val, @Nullable Map<String, Object> options) {
    // handle the first time
    int oldLiveness = -55;
    if (liveness.containsKey(i)) {
      oldLiveness = liveness.get(i);
    }

    if (val != oldLiveness) {
      liveness.put(i, val);
      notifyLivenessListeners(i, val, options);
    }
  }

  // todo, make this a timer weak hash map
  private final Map<UpperIdentifier, Integer> liveness = new HashMap<UpperIdentifier, Integer>();

  protected void notifyLivenessListeners(UpperIdentifier i, int liveness, @Nullable Map<String, Object> options) {
    logger.trace("notifyLivenessListeners({},{})", i, liveness);
    for (LivenessListener<UpperIdentifier> listener : livenessListeners) {
      listener.livenessChanged(i, liveness, options);
    }
  }

  final Collection<ProximityListener<UpperIdentifier>> proxListeners =
    new CopyOnWriteArrayList<ProximityListener<UpperIdentifier>>();
  @Override
  public void addProximityListener(ProximityListener<UpperIdentifier> name) {
    proxListeners.add(name);
  }

  @Override
  public boolean removeProximityListener(ProximityListener<UpperIdentifier> name) {
    return proxListeners.remove(name);
  }

  @Override
  public int proximity(UpperIdentifier i, @Nullable Map<String, Object> options) {
    logger.trace("proximity({})", i);
    if (deadForever.contains(i)) return Integer.MAX_VALUE;
    return prox.proximity(serializer.translateDown(i), OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, i));
  }

  @Override
  public void proximityChanged(MiddleIdentifier i, int newProx, @Nullable Map<String, Object> options) {
    UpperIdentifier upper = identity.getIntendedDest(options);
    if (upper == null) {
      logger.atWarn().setCause(new Exception("Stack Trace")).log(() -> "Memory for "+IdentityImpl.index(options)+" collected suppressing proximityChanged()");
      return;
    }

    notifyProximityListeners(upper, newProx, options);
  }

  private void notifyProximityListeners(UpperIdentifier i, int newProx, @Nullable Map<String, Object> options) {
    logger.trace("notifyProximityListeners({},{})", i, newProx);
    for (ProximityListener<UpperIdentifier> listener : proxListeners) {
      listener.proximityChanged(i, newProx, options);
    }
  }

  @Override
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  @Override
  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  @Override
  public UpperIdentifier getLocalIdentifier() {
    return localIdentifier;
  }

  @Override
  public void setCallback(TransportLayerCallback<UpperIdentifier, UpperMsgType> callback) {
    this.callback = callback;
  }

  @Override
  public void setErrorHandler(ErrorHandler<UpperIdentifier> handler) {
    this.errorHandler = handler;
  }

  @Override
  public void destroy() {
    logger.info("destroy()");
    tl.destroy();
  }
}
