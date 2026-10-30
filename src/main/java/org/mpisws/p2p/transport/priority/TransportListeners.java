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

import java.util.ArrayList;
import java.util.Map;

import org.mpisws.p2p.transport.TransportLayerListener;

/**
 * The statistics listeners of a {@link PriorityTransportLayerImpl}: they hear of every message read,
 * written, enqueued and dropped. Listeners are notified on a copy of the list, so they may unregister
 * from within a callback.
 */
final class TransportListeners<Identifier> {

  /**
   * Note that listeners contains a superset of plisteners
   */
  private final ArrayList<TransportLayerListener<Identifier>> listeners = new ArrayList<TransportLayerListener<Identifier>>();
  private final ArrayList<PriorityTransportLayerListener<Identifier>> plisteners = new ArrayList<PriorityTransportLayerListener<Identifier>>();

  void addTransportLayerListener(TransportLayerListener<Identifier> listener) {
    synchronized(listeners) {
      listeners.add(listener);
    }
  }

  void removeTransportLayerListener(TransportLayerListener<Identifier> listener) {
    synchronized(listeners) {
      listeners.remove(listener);
    }
  }

  void addPriorityTransportLayerListener(PriorityTransportLayerListener<Identifier> listener) {
    synchronized(plisteners) {
      plisteners.add(listener);
    }
    addTransportLayerListener(listener);
  }

  void removePriorityTransportLayerListener(PriorityTransportLayerListener<Identifier> listener) {
    synchronized(plisteners) {
      plisteners.remove(listener);
    }
    removeTransportLayerListener(listener);
  }

  void notifyRead(int size, Identifier source,
      Map<String, Object> options) {

    if (listeners.isEmpty()) return;
    ArrayList<TransportLayerListener<Identifier>> temp;

    synchronized(listeners) {
      temp = new ArrayList<TransportLayerListener<Identifier>>(listeners);
    }
    for (TransportLayerListener<Identifier> l : temp) {
      l.read(size, source, options, true, true);
    }
  }

  void notifyWrote(int size, Identifier dest,
      Map<String, Object> options) {
    if (listeners.isEmpty()) return;
    ArrayList<TransportLayerListener<Identifier>> temp;

    synchronized(listeners) {
      temp = new ArrayList<TransportLayerListener<Identifier>>(listeners);
    }
    for (TransportLayerListener<Identifier> l : temp) {
      l.wrote(size, dest, options, true, true);
    }
  }

  void notifyEnqueued(int size, Identifier dest,
      Map<String, Object> options) {
    if (plisteners.isEmpty()) return;
    ArrayList<PriorityTransportLayerListener<Identifier>> temp;

    synchronized(plisteners) {
      temp = new ArrayList<PriorityTransportLayerListener<Identifier>>(plisteners);
    }
    for (PriorityTransportLayerListener<Identifier> l : temp) {
      l.enqueued(size, dest, options, true, true);
    }
  }

  void notifyDropped(int size, Identifier dest,
      Map<String, Object> options) {
    if (plisteners.isEmpty()) return;
    ArrayList<PriorityTransportLayerListener<Identifier>> temp;

    synchronized(plisteners) {
      temp = new ArrayList<PriorityTransportLayerListener<Identifier>>(plisteners);
    }
    for (PriorityTransportLayerListener<Identifier> l : temp) {
      l.dropped(size, dest, options, true, true);
    }
  }
}
