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
      Runnable r = () -> {
        addBinding(handle, null, null); // I hope setting the options to null is ok...
      };
      if (IdentityLayerCore.this.environment.getSelectorManager().isSelectorThread()) {
        r.run();
      } else {
        IdentityLayerCore.this.environment.getSelectorManager().invoke(r);
      }
    });
  }

  public void addPendingMessage(UpperIdentifier i, IdentityMessageHandle ret) {
    logger.trace("addPendingMessage({},{})", i, ret);
    synchronized(pendingMessages) {
      Set<IdentityMessageHandle> set = pendingMessages.get(i);
      if (set == null) {
        set = new HashSet<IdentityMessageHandle>();
        pendingMessages.put(i, set);
      }
      set.add(ret);
    }
  }

  public void removePendingMessage(UpperIdentifier i, IdentityMessageHandle ret) {
    synchronized(pendingMessages) {
      Set<IdentityMessageHandle> set = pendingMessages.get(i);
      if (set == null) {
        return;
      }
      set.remove(ret);
      if (set.isEmpty()) {
        pendingMessages.remove(i);
      }
    }
  }

  @SuppressWarnings("NullAway.Init") // set by setOverrideLiveness() before the layer is used
  OverrideLiveness<LowerIdentifier> overrideLiveness;
  public void setOverrideLiveness(OverrideLiveness<LowerIdentifier> ol) {
    this.overrideLiveness = ol;
  }

  /**
   * Put this in lower.
   */
  public void setDeadForever(@Nullable LowerIdentifier l, UpperIdentifier i, @Nullable Map<String, Object> options) {
    if (deadForever.contains(i)) return;
    if (logger.isTraceEnabled()) logger.trace("setDeadForever({},{},{})", l, i, options, new Exception("Stack Trace"));
    deadForever.add(i);
      Map<String, Object> o2 = OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, i);
      // how do we notify?  If we have a lower, then let it trickle through the TLs, otherwise, call it only on the upper
      if (l == null) {
        logger.info("setDeadForever({},{},{}): l == null", l, i, options);
        upper().setLiveness(i, LIVENESS_DEAD_FOREVER, o2);
      } else {
        overrideLiveness.setLiveness(l, LIVENESS_DEAD_FOREVER, o2);
      }
    Set<IdentityMessageHandle> cancelMe = pendingMessages.remove(i);
    if (cancelMe != null) {
      for (IdentityMessageHandle msg : cancelMe) {
        msg.deadForever();
      }
    }
    upper().clearState(i);
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
   * @param l is optional, and is set when the lower-layer calls it
   * @return false if the new binding is actually old (IE, don't upgrade it)
   */
  protected boolean addBinding(UpperIdentifier u, @Nullable LowerIdentifier l, @Nullable Map<String, Object> options) {
    if (!environment.getSelectorManager().isSelectorThread()) throw new RuntimeException("Must be called on selector thread.");
    // also for wire-deserialized handles (null options): the strategy decides what it accepts without options
    if (bindStrategy != null) {
      if (!bindStrategy.accept(u, l, options)) return false;
    }
    MiddleIdentifier m = serializer.translateDown(u);
      if (deadForever.contains(u)) return false;

      UpperIdentifier old = bindings.get(m);
      if (old == null) {
        logger.debug("addBinding({},{}) old is null", u, l);
        bindings.put(m, u);
        keepIfStrategyKeeps(m, u, options);
        forgetBindingsOverCap();
        if (l != null) {
          overrideLiveness.setLiveness(l, LIVENESS_ALIVE, OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, u));
        } else {
          upper().setLiveness(u, LIVENESS_ALIVE, OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, u));
        }
        return true;
      } else {
        if (old.equals(u)) {
          logger.trace("addBinding({},{}) old is equal", u, l);
          keepIfStrategyKeeps(m, u, options);
          if (l != null) {
            overrideLiveness.setLiveness(l, LIVENESS_ALIVE, OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, u));
          } else {
            upper().setLiveness(u, LIVENESS_ALIVE, OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, u));
          }
          return true;
        }

        // they are different
        BindStrategy.Replacement replacement = bindStrategy == null
            ? BindStrategy.Replacement.ASK_NODE_CHANGE_STRATEGY
            : bindStrategy.replace(old, u, options);
        if (replacement == BindStrategy.Replacement.KEEP) {
          logger.info("addBinding({},{}): the bind strategy keeps {}", u, l, old);
          return false;
        }
        boolean replace;
        if (replacement == BindStrategy.Replacement.REPLACE) {
          logger.info("addBinding({},{}): the bind strategy replaces {}", u, l, old);
          setDeadForever(l, old, options);
          replace = true;
        } else {
          replace = destinationChanged(old, u, l, options);
        }
        if (replace) {
          bindings.put(m, u);
          keptBindings.remove(m);
          keepIfStrategyKeeps(m, u, options);
          logger.debug("addBinding({},{}) old {} is dead", u, l, old);
          if (l == null) {
            logger.info("addBinding({},{}): l == null", u, l);
            upper().setLiveness(u, LIVENESS_ALIVE, OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, u));
          } else {
            overrideLiveness.setLiveness(l, LIVENESS_ALIVE, OptionsFactory.addOption(options, NODE_HANDLE_FROM_INDEX, u));
          }
          return true;
        } else {
          // mark the new one as faulty
          logger.info("The nodeChangeStrategy found identifier {} to be stale.  Should be using {}", u, old);
          return false;
        }
      }
  }

  private void keepIfStrategyKeeps(MiddleIdentifier m, UpperIdentifier u, @Nullable Map<String, Object> options) {
    if (bindStrategy != null && bindStrategy.shouldKeepBinding(u, options)) {
      keptBindings.add(m);
    }
  }

  /**
   * Forgets the least recently used bindings until at most {@link #maxBindings} are left, skipping those the bind
   * strategy keeps and those with pending messages. A forgotten binding is made again by the next message from its
   * address.
   */
  private void forgetBindingsOverCap() {
    Iterator<Map.Entry<MiddleIdentifier, UpperIdentifier>> it = bindings.entrySet().iterator();
    while (bindings.size() > maxBindings && it.hasNext()) {
      Map.Entry<MiddleIdentifier, UpperIdentifier> binding = it.next();
      if (keptBindings.contains(binding.getKey()) || hasPendingMessages(binding.getValue())) {
        continue;
      }
      logger.debug("forgetting the binding {} -> {}", binding.getKey(), binding.getValue());
      it.remove();
    }
  }

  private boolean hasPendingMessages(UpperIdentifier u) {
    synchronized(pendingMessages) {
      return pendingMessages.containsKey(u);
    }
  }

  /**
   * Put this in lower.
   *
   * Return true if the new one is a valid replacement of the old
   *
   * True if they are the same
   */
  public boolean destinationChanged(UpperIdentifier oldDest, UpperIdentifier newDest, @Nullable LowerIdentifier i, @Nullable Map<String, Object> options) {
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
        logger.info("destinationChanged({}->{},{},{})", oldDest, newDest, i, options);
        setDeadForever(i, oldDest, options);
        return true;
      } else {
        logger.info("destinationDidntChange({}->{},{},{})", newDest, oldDest, i, options);
        setDeadForever(i, newDest, options);
        return false;
      }
    }
  }

  public void initLowerLayer(TransportLayer<LowerIdentifier, ByteBuffer> tl, ErrorHandler<LowerIdentifier> handler) {
    lower = new LowerIdentityImpl<>(this, tl, handler);
  }

  public @Nullable LowerIdentity<LowerIdentifier, ByteBuffer> getLowerIdentity() {
    return lower;
  }



  public @Nullable UpperIdentity<UpperIdentifier, UpperMsgType> getUpperIdentity() {
    return upper;
  }

  public void initUpperLayer(UpperIdentifier localIdentifier, TransportLayer<MiddleIdentifier, UpperMsgType> tl,
      LivenessProvider<MiddleIdentifier> live,
      ProximityProvider<MiddleIdentifier> prox,
      OverrideLiveness<LowerIdentifier> overrideLiveness) {
    if (upper != null) throw new IllegalStateException("upper already initialized:"+upper);
    upper = new UpperIdentityImpl<>(this, localIdentifier, tl, live, prox);

    setOverrideLiveness(overrideLiveness);
  }



  class IdentityMessageHandle extends MessageRequestHandleImpl<UpperIdentifier, UpperMsgType> implements MessageCallback<MiddleIdentifier, UpperMsgType> {

    private final @Nullable MessageCallback<UpperIdentifier, UpperMsgType> deliverAckToMe;

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
      removePendingMessage(getIdentifier(), this);
      return super.cancel();
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
