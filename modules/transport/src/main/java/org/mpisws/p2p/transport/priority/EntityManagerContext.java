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
package org.mpisws.p2p.transport.priority;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.slf4j.Logger;
import rice.selector.SelectorManager;

/**
 * What an {@link EntityManager} needs from its {@link PriorityTransportLayer}: the layer below, the
 * liveness and proximity providers, the size limits, and the layer's mutable settings (callback, error
 * handler, destroyed flag), which are read through suppliers because they may change after the manager
 * was built.
 */
final class EntityManagerContext<Identifier> {
  final TransportLayer<Identifier, ByteBuffer> tl;
  final LivenessProvider<Identifier> livenessProvider;
  final ProximityProvider<Identifier> proximityProvider;
  final SelectorManager selectorManager;
  final Logger logger;
  final int maxMsgSize;
  final int maxQueueSize;
  final int maxBigMsgSize;
  final List<PrimarySocketListener<Identifier>> primarySocketListeners;
  final TransportListeners<Identifier> listeners;
  final Supplier<ErrorHandler<Identifier>> errorHandler;
  final Supplier<TransportLayerCallback<Identifier, ByteBuffer>> callback;
  final BooleanSupplier destroyed;

  /**
   * looks up (or creates) the manager of an identifier
   */
  final Function<Identifier, EntityManager<Identifier>> entityManagers;

  EntityManagerContext(TransportLayer<Identifier, ByteBuffer> tl,
      LivenessProvider<Identifier> livenessProvider,
      ProximityProvider<Identifier> proximityProvider,
      SelectorManager selectorManager,
      Logger logger,
      int maxMsgSize,
      int maxQueueSize,
      int maxBigMsgSize,
      List<PrimarySocketListener<Identifier>> primarySocketListeners,
      TransportListeners<Identifier> listeners,
      Supplier<ErrorHandler<Identifier>> errorHandler,
      Supplier<TransportLayerCallback<Identifier, ByteBuffer>> callback,
      BooleanSupplier destroyed,
      Function<Identifier, EntityManager<Identifier>> entityManagers) {
    this.tl = tl;
    this.livenessProvider = livenessProvider;
    this.proximityProvider = proximityProvider;
    this.selectorManager = selectorManager;
    this.logger = logger;
    this.maxMsgSize = maxMsgSize;
    this.maxQueueSize = maxQueueSize;
    this.maxBigMsgSize = maxBigMsgSize;
    this.primarySocketListeners = primarySocketListeners;
    this.listeners = listeners;
    this.errorHandler = errorHandler;
    this.callback = callback;
    this.destroyed = destroyed;
    this.entityManagers = entityManagers;
  }
}
