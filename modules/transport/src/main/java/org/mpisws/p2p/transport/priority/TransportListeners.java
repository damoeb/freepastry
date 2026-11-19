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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.TransportLayerListener;

/**
 * The statistics listeners of a {@link PriorityTransportLayer}: they hear of every message read,
 * written, enqueued and dropped. Listeners are notified on a copy of the list, so they may unregister
 * from within a callback.
 */
final class TransportListeners<Identifier> {

  /**
   * Note that listeners contains a superset of plisteners
   */
  private final List<TransportLayerListener<Identifier>> listeners = new CopyOnWriteArrayList<TransportLayerListener<Identifier>>();
  private final List<PriorityTransportLayerListener<Identifier>> plisteners = new CopyOnWriteArrayList<PriorityTransportLayerListener<Identifier>>();

  void addTransportLayerListener(TransportLayerListener<Identifier> listener) {
    listeners.add(listener);
  }

  void removeTransportLayerListener(TransportLayerListener<Identifier> listener) {
    listeners.remove(listener);
  }

  void addPriorityTransportLayerListener(PriorityTransportLayerListener<Identifier> listener) {
    plisteners.add(listener);
    addTransportLayerListener(listener);
  }

  void removePriorityTransportLayerListener(PriorityTransportLayerListener<Identifier> listener) {
    plisteners.remove(listener);
    removeTransportLayerListener(listener);
  }

  void notifyRead(int size, Identifier source,
      @Nullable Map<String, Object> options) {

    if (listeners.isEmpty()) return;
    for (TransportLayerListener<Identifier> transportLayerListener : listeners) {
      transportLayerListener.read(size, source, options, true, true);
    }
  }

  void notifyWrote(int size, Identifier destination,
      @Nullable Map<String, Object> options) {
    if (listeners.isEmpty()) return;
    for (TransportLayerListener<Identifier> transportLayerListener : listeners) {
      transportLayerListener.wrote(size, destination, options, true, true);
    }
  }

  void notifyEnqueued(int size, Identifier destination,
      @Nullable Map<String, Object> options) {
    if (plisteners.isEmpty()) return;
    for (PriorityTransportLayerListener<Identifier> priorityTransportLayerListener : plisteners) {
      priorityTransportLayerListener.enqueued(size, destination, options, true, true);
    }
  }

  void notifyDropped(int size, Identifier destination,
      @Nullable Map<String, Object> options) {
    if (plisteners.isEmpty()) return;
    for (PriorityTransportLayerListener<Identifier> priorityTransportLayerListener : plisteners) {
      priorityTransportLayerListener.dropped(size, destination, options, true, true);
    }
  }
}
