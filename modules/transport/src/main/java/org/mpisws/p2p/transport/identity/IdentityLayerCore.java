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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import rice.p2p.commonapi.Cancellable;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.OverrideLiveness;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.slf4j.Logger;
import rice.environment.Environment;

import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_ALIVE;
import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_DEAD_FOREVER;
import static rice.environment.params.ConfigChecks.requirePositive;

public class IdentityLayerCore<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier> {
  final byte[] localIdentifier;

  private @Nullable LowerIdentityImpl<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier> lower;
  private @Nullable UpperIdentityImpl<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier> upper;

  private final Map<UpperIdentifier, Set<IdentityMessageHandle>> pendingMessages;
  /** at most {@link #maxBindings}, the least recently marked forgotten first */
  final Set<UpperIdentifier> deadForever;

  final Environment environment;
  private final Logger logger;

  final IdentitySerializer<UpperIdentifier, MiddleIdentifier, LowerIdentifier> serializer;
  private final NodeChangeStrategy<UpperIdentifier> nodeChangeStrategy;
  final SanityChecker<UpperIdentifier, MiddleIdentifier> sanityChecker;

  /**
   * Multiple UpperIdentifiers claim the same binding to the
   * LowerIdentifier.
   *
   * If these are unrelated nodes (different ID/credentials) we need to
   * make sure to expire the node first.  If they are the same node, and
   * just a new epoch, then we can expire them right away.
   *
   * Note, that it's possible to have the UpperIdentifier in multiple places if it
   * has multiple paths (such as source routing)
   *
   * Least recently used first. Every datagram's identity header offers a binding, so beyond {@link #maxBindings}
   * the least recently used ones are forgotten, except those in {@link #keptBindings} and those with pending
   * messages. Selector thread only.
   */
  final Map<MiddleIdentifier, UpperIdentifier> bindings;  // this one may be difficult as the Upper has a ref to the lower

  /** the bindings the bind strategy keeps: never forgotten while their upper identifier is bound */
  private final Set<MiddleIdentifier> keptBindings;

  /** {@code pastry_socket_identity_max_bindings}: the most bindings (and dead-forever identifiers) remembered */
  final int maxBindings;

  public static final byte SUCCESS = 1;
  public static final byte FAILURE = 0;

  public static final byte NO_ID = 2;
  public static final byte NORMAL = 1;
  public static final byte INCORRECT_IDENTITY = 0;

  public static final String NODE_HANDLE_FROM_INDEX = "identity.node_handle_to_index";
  public static final String DONT_VERIFY = "identity.dont_verify_dest";

  final BindStrategy<UpperIdentifier, LowerIdentifier> bindStrategy;

  public IdentityLayerCore(
      byte[] localIdentifier,
      IdentitySerializer<UpperIdentifier, MiddleIdentifier, LowerIdentifier> serializer,
      NodeChangeStrategy<UpperIdentifier> nodeChangeStrategy,
      SanityChecker<UpperIdentifier, MiddleIdentifier> sanityChecker,
      BindStrategy<UpperIdentifier, LowerIdentifier> bindStrategy,
      Environment environment) {
    this.logger = environment.getLogManager().getLogger(IdentityLayerCore.class, null);
    this.bindStrategy = bindStrategy;
    this.sanityChecker = sanityChecker;
    if (sanityChecker == null) throw new IllegalArgumentException("SanityChecker is null");
    this.localIdentifier = localIdentifier;
    this.serializer = serializer;
    this.nodeChangeStrategy = nodeChangeStrategy;
    this.environment = environment;

    this.maxBindings = requirePositive("pastry_socket_identity_max_bindings",
        environment.getParameters().getInt("pastry_socket_identity_max_bindings"));
    this.pendingMessages = new HashMap<UpperIdentifier, Set<IdentityMessageHandle>>();
    this.deadForever = Collections.synchronizedSet(Collections.newSetFromMap(new LinkedHashMap<UpperIdentifier, Boolean>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<UpperIdentifier, Boolean> eldest) {
        return size() > maxBindings;
      }
    }));

    this.bindings = new LinkedHashMap<MiddleIdentifier, UpperIdentifier>(16, 0.75f, true);
    this.keptBindings = new HashSet<MiddleIdentifier>();

    serializer.addSerializerListener(handle -> {
      Runnable task = () -> {
        addBinding(handle, null, null); // I hope setting the options to null is ok...
      };
      if (IdentityLayerCore.this.environment.getSelectorManager().isSelectorThread()) {
        task.run();
      } else {
        IdentityLayerCore.this.environment.getSelectorManager().invoke(task);
      }
    });
  }

  public void addPendingMessage(UpperIdentifier upperIdentifier, IdentityMessageHandle ret) {
    logger.trace("addPendingMessage({},{})", upperIdentifier, ret);
    synchronized(pendingMessages) {
      Set<IdentityMessageHandle> set = pendingMessages.get(upperIdentifier);
      if (set == null) {
        set = new HashSet<IdentityMessageHandle>();
        pendingMessages.put(upperIdentifier, set);
      }
      set.add(ret);
    }
  }

  public void removePendingMessage(UpperIdentifier upperIdentifier, IdentityMessageHandle ret) {
    synchronized(pendingMessages) {
      Set<IdentityMessageHandle> set = pendingMessages.get(upperIdentifier);
      if (set == null) {
        return;
      }
      set.remove(ret);
      if (set.isEmpty()) {
        pendingMessages.remove(upperIdentifier);
      }
    }
  }

  @SuppressWarnings("NullAway.Init") // set by setOverrideLiveness() before the layer is used
  OverrideLiveness<LowerIdentifier> overrideLiveness;
  public void setOverrideLiveness(OverrideLiveness<LowerIdentifier> overrideLiveness) {
    this.overrideLiveness = overrideLiveness;
  }

  /**
   * Put this in lower.
   */
  public void setDeadForever(@Nullable LowerIdentifier lowerIdentifier, UpperIdentifier upperIdentifier, @Nullable Map<String, Object> options) {
    if (deadForever.contains(upperIdentifier)) return;
    if (logger.isTraceEnabled()) logger.trace("setDeadForever({},{},{})", lowerIdentifier, upperIdentifier, options, new Exception("Stack Trace"));
    deadForever.add(upperIdentifier);
      Map<String, Object> o2 = OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, upperIdentifier);
      // how do we notify?  If we have a lower, then let it trickle through the TLs, otherwise, call it only on the upper
      if (lowerIdentifier == null) {
        logger.info("setDeadForever({},{},{}): l == null", lowerIdentifier, upperIdentifier, options);
        upper().setLiveness(upperIdentifier, LIVENESS_DEAD_FOREVER, o2);
      } else {
        overrideLiveness.setLiveness(lowerIdentifier, LIVENESS_DEAD_FOREVER, o2);
      }
    Set<IdentityMessageHandle> cancelMe = pendingMessages.remove(upperIdentifier);
    if (cancelMe != null) {
      for (IdentityMessageHandle msg : cancelMe) {
        msg.deadForever();
      }
    }
    upper().clearState(upperIdentifier);
  }

  private UpperIdentityImpl<UpperIdentifier, MiddleIdentifier, UpperMsgType, LowerIdentifier> upper() {
    return Objects.requireNonNull(upper, "initUpperLayer() was not called");
  }

  /** The upper identifier stored in the options, for log messages. */
  static @Nullable Object index(@Nullable Map<String, Object> options) {
    return options == null ? null : options.get(NODE_HANDLE_FROM_INDEX);
  }

  protected UpperIdentifier getIntendedDest(@Nullable Map<String,Object> options) {
    if (options == null) {
      throw new IllegalArgumentException("options is null");
    }
    if (!options.containsKey(NODE_HANDLE_FROM_INDEX)) throw new IllegalArgumentException("options doesn't have NODE_HANDLE_FROM_INDEX "+options);

    return (UpperIdentifier)options.get(NODE_HANDLE_FROM_INDEX);
  }

  /**
   * @param lowerIdentifier is optional, and is set when the lower-layer calls it
   * @return false if the new binding is actually old (IE, don't upgrade it)
   */
  protected boolean addBinding(UpperIdentifier upperIdentifier, @Nullable LowerIdentifier lowerIdentifier, @Nullable Map<String, Object> options) {
    if (!environment.getSelectorManager().isSelectorThread()) throw new RuntimeException("Must be called on selector thread.");
    // also for wire-deserialized handles (null options): the strategy decides what it accepts without options
    if (bindStrategy != null) {
      if (!bindStrategy.accept(upperIdentifier, lowerIdentifier, options)) return false;
    }
    MiddleIdentifier middleIdentifier = serializer.translateDown(upperIdentifier);
      if (deadForever.contains(upperIdentifier)) return false;

      UpperIdentifier old = bindings.get(middleIdentifier);
      if (old == null) {
        logger.debug("addBinding({},{}) old is null", upperIdentifier, lowerIdentifier);
        bindings.put(middleIdentifier, upperIdentifier);
        keepIfStrategyKeeps(middleIdentifier, upperIdentifier, options);
        forgetBindingsOverCap();
        markAlive(upperIdentifier, lowerIdentifier, options);
        return true;
      } else {
        if (old.equals(upperIdentifier)) {
          logger.trace("addBinding({},{}) old is equal", upperIdentifier, lowerIdentifier);
          keepIfStrategyKeeps(middleIdentifier, upperIdentifier, options);
          markAlive(upperIdentifier, lowerIdentifier, options);
          return true;
        }

        // they are different
        BindStrategy.Replacement replacement = bindStrategy == null
            ? BindStrategy.Replacement.ASK_NODE_CHANGE_STRATEGY
            : bindStrategy.replace(old, upperIdentifier, options);
        if (replacement == BindStrategy.Replacement.KEEP) {
          logger.info("addBinding({},{}): the bind strategy keeps {}", upperIdentifier, lowerIdentifier, old);
          return false;
        }
        boolean replace;
        if (replacement == BindStrategy.Replacement.REBIND) {
          logger.info("addBinding({},{}): the bind strategy rebinds from {}, which stays alive", upperIdentifier, lowerIdentifier, old);
          replace = true;
        } else if (replacement == BindStrategy.Replacement.REPLACE) {
          logger.info("addBinding({},{}): the bind strategy replaces {}", upperIdentifier, lowerIdentifier, old);
          setDeadForever(lowerIdentifier, old, options);
          replace = true;
        } else {
          replace = destinationChanged(old, upperIdentifier, lowerIdentifier, options);
        }
        if (replace) {
          bindings.put(middleIdentifier, upperIdentifier);
          keptBindings.remove(middleIdentifier);
          keepIfStrategyKeeps(middleIdentifier, upperIdentifier, options);
          logger.debug("addBinding({},{}) old {} is dead", upperIdentifier, lowerIdentifier, old);
          if (lowerIdentifier == null) {
            logger.info("addBinding({},{}): l == null", upperIdentifier, lowerIdentifier);
          }
          markAlive(upperIdentifier, lowerIdentifier, options);
          return true;
        } else {
          // mark the new one as faulty
          logger.info("The nodeChangeStrategy found identifier {} to be stale.  Should be using {}", upperIdentifier, old);
          return false;
        }
      }
  }

  /**
   * Marks the upper identifier alive after its binding was made or confirmed. With a lower identifier (it came over a
   * connection or in a datagram) the lower identifier, the route, is marked alive in the liveness layer, which skips
   * that layer's own check (a pong must answer a ping this node sent): so only if the bind strategy finds that the
   * channel proves the route ({@link BindStrategy#provesLiveness}).
   */
  private void markAlive(UpperIdentifier upperIdentifier, @Nullable LowerIdentifier lowerIdentifier, @Nullable Map<String, Object> options) {
    if (lowerIdentifier == null) {
      upper().setLiveness(upperIdentifier, LIVENESS_ALIVE, OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, upperIdentifier));
    } else if (bindStrategy == null || bindStrategy.provesLiveness(upperIdentifier, options)) {
      overrideLiveness.setLiveness(lowerIdentifier, LIVENESS_ALIVE, OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, upperIdentifier));
    }
  }

  private void keepIfStrategyKeeps(MiddleIdentifier middleIdentifier, UpperIdentifier upperIdentifier, @Nullable Map<String, Object> options) {
    if (bindStrategy != null && bindStrategy.shouldKeepBinding(upperIdentifier, options)) {
      keptBindings.add(middleIdentifier);
    }
  }

  /**
   * Forgets the least recently used bindings until at most {@link #maxBindings} are left, skipping those the bind
   * strategy keeps and those with pending messages. A forgotten binding is made again by the next message from its
   * address.
   */
  private void forgetBindingsOverCap() {
    Iterator<Map.Entry<MiddleIdentifier, UpperIdentifier>> iterator = bindings.entrySet().iterator();
    while (bindings.size() > maxBindings && iterator.hasNext()) {
      Map.Entry<MiddleIdentifier, UpperIdentifier> binding = iterator.next();
      if (keptBindings.contains(binding.getKey()) || hasPendingMessages(binding.getValue())) {
        continue;
      }
      logger.debug("forgetting the binding {} -> {}", binding.getKey(), binding.getValue());
      iterator.remove();
    }
  }

  private boolean hasPendingMessages(UpperIdentifier upperIdentifier) {
    synchronized(pendingMessages) {
      return pendingMessages.containsKey(upperIdentifier);
    }
  }

  /**
   * Put this in lower.
   *
   * Return true if the new one is a valid replacement of the old
   *
   * True if they are the same
   */
  public boolean destinationChanged(UpperIdentifier oldDest, UpperIdentifier newDest, @Nullable LowerIdentifier lowerIdentifier, @Nullable Map<String, Object> options) {
    if (oldDest.equals(newDest)) {
      // don't do anything
      return true;
    } else {
      if (deadForever.contains(oldDest)) {
        return true;
      }
      if (deadForever.contains(newDest)) {
        return false;
      }

      if (nodeChangeStrategy.isChangeAllowed(oldDest, newDest)) {
        logger.info("destinationChanged({}->{},{},{})", oldDest, newDest, lowerIdentifier, options);
        setDeadForever(lowerIdentifier, oldDest, options);
        return true;
      } else {
        logger.info("destinationDidntChange({}->{},{},{})", newDest, oldDest, lowerIdentifier, options);
        setDeadForever(lowerIdentifier, newDest, options);
        return false;
      }
    }
  }

  public void initLowerLayer(TransportLayer<LowerIdentifier, ByteBuffer> transportLayer, ErrorHandler<LowerIdentifier> handler) {
    lower = new LowerIdentityImpl<>(this, transportLayer, handler);
  }

  public @Nullable LowerIdentity<LowerIdentifier, ByteBuffer> getLowerIdentity() {
    return lower;
  }



  public @Nullable UpperIdentity<UpperIdentifier, UpperMsgType> getUpperIdentity() {
    return upper;
  }

  public void initUpperLayer(UpperIdentifier localIdentifier, TransportLayer<MiddleIdentifier, UpperMsgType> transportLayer,
      LivenessProvider<MiddleIdentifier> live,
      ProximityProvider<MiddleIdentifier> prox,
      OverrideLiveness<LowerIdentifier> overrideLiveness) {
    if (upper != null) throw new IllegalStateException("upper already initialized:"+upper);
    upper = new UpperIdentityImpl<>(this, localIdentifier, transportLayer, live, prox);

    setOverrideLiveness(overrideLiveness);
  }



  class IdentityMessageHandle extends MessageRequestHandleImpl<UpperIdentifier, UpperMsgType> implements MessageCallback<MiddleIdentifier, UpperMsgType> {

    private final @Nullable MessageCallback<UpperIdentifier, UpperMsgType> deliverAckToMe;
    private boolean cancelRequested = false;

    public IdentityMessageHandle(
        UpperIdentifier identifier,
        UpperMsgType message, @Nullable Map<String, Object> options,
        @Nullable MessageCallback<UpperIdentifier, UpperMsgType> deliverAckToMe) {
      super(identifier, message, options);
      this.deliverAckToMe = deliverAckToMe;
    }

    void deadForever() {
      cancel();
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(this, new NodeIsFaultyException(getIdentifier(),getMessage()));
    }

    /**
     * Forgets the pending message, then cancels the lower send.
     */
    @Override
    public boolean cancel() {
      cancelRequested = true;
      removePendingMessage(getIdentifier(), this);
      return super.cancel();
    }

    /**
     * The lower layer can find the peer dead while still inside sendMessage(), before it returned its handle; the
     * message it is about to send must then be cancelled as soon as the handle arrives.
     */
    @Override
    public void setSubCancellable(@Nullable Cancellable cancellable) {
      super.setSubCancellable(cancellable);
      if (cancelRequested && cancellable != null) cancellable.cancel();
    }

    @Override
    public void ack(MessageRequestHandle<MiddleIdentifier, UpperMsgType> msg) {
      removePendingMessage(getIdentifier(), this);
      if (deliverAckToMe != null) deliverAckToMe.ack(this);
    }

    @Override
    public void sendFailed(MessageRequestHandle<MiddleIdentifier, UpperMsgType> msg, Exception reason) {
      removePendingMessage(getIdentifier(), this);
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(this, reason);
    }

    @Override
    public String toString() {
      return "IdMsgHdl{"+getMessage()+"}->"+getIdentifier();
    }
  }
}
