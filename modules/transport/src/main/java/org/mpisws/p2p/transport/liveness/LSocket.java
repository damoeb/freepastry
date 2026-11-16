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
package org.mpisws.p2p.transport.liveness;

import java.io.IOException;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import rice.selector.TimerTask;

import static org.mpisws.p2p.transport.liveness.LivenessTypes.LIVENESS_SUSPECTED;
/**
 * The purpose of this class is to checkliveness on a stalled socket that we are waiting to write on.
 *
 * the livenessCheckerTimer is set every time we want to write, and killed every time we do write
 *
 * TODO: think about exactly what we want to use for the delay on the timer, currently using rto*4
 *
 * @author Jeff Hoye
 */
class LSocket<Identifier> extends SocketWrapperSocket<Identifier, Identifier> {
  final LivenessTransportLayer<Identifier> layer;
  LivenessTracker<Identifier> manager;

  /**
   * This is for memory management, so that we don't collect the identifier in the LivenessTracker
   * while we still have open sockets.
   */
  Identifier hardRef;

  /**
   * set every time we want to write, and killed every time we do write
   */
  @Nullable TimerTask livenessCheckerTimer;

  boolean closed = false;

  public LSocket(LivenessTransportLayer<Identifier> layer, LivenessTracker<Identifier> manager, P2PSocket<Identifier> socket, @Nullable Identifier hardRef) {
    super(socket.getIdentifier(), socket, layer.logger, layer.errorHandler, socket.getOptions());
    this.layer = layer;
    if (hardRef == null) throw new IllegalArgumentException("hardRef == null "+manager+" "+socket);
    this.manager = manager;
    this.hardRef = hardRef;
  }

  // done with call to close

  @Override
  public void register(boolean wantToRead, boolean wantToWrite, final P2PSocketReceiver<Identifier> receiver) {
    if (closed) {
      receiver.receiveException(this, new ClosedChannelException("Socket "+this+" is already closed."));
      return;
    }
    if (wantToWrite) startLivenessCheckerTimer();
    super.register(wantToRead, wantToWrite, receiver);
  }

  @Override
  public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
    LivenessTracker<Identifier> m = layer.getManager(socket.getIdentifier());
    if (m.liveness > LIVENESS_SUSPECTED) {
      m.updated = 0L;
      m.checkLiveness(socket.getOptions());
    }
    if (canWrite) {
      stopLivenessCheckerTimer();
    }
    super.receiveSelectResult(socket, canRead, canWrite);
  }


  public void startLivenessCheckerTimer() {
    // it's already going to check
    synchronized(LSocket.this) {
      if (livenessCheckerTimer != null) return;
      livenessCheckerTimer = new TimerTask(){
        @Override
        @SuppressWarnings("ReferenceEquality") // the same instance is meant
        public void run() {
          synchronized(LSocket.this) {
            if (livenessCheckerTimer == this) livenessCheckerTimer = null;
          }
          manager.checkLiveness(options);
        }
      };
    } // sync
    logger.atTrace().log(() -> "Checking liveness on "+manager.identifier.get()+" in "+manager.rto()+" millis if we don't write.");
    layer.timer.schedule(livenessCheckerTimer, (long) manager.rto()*LivenessTransportLayer.IDLE_CHECK_RTO_MULTIPLIER, LivenessTransportLayer.IDLE_CHECK_PERIOD_MILLIS);
  }

  public void stopLivenessCheckerTimer() {
    synchronized(LSocket.this) {
      if (livenessCheckerTimer != null) livenessCheckerTimer.cancel();
      livenessCheckerTimer = null;
    }
  }

  @Override
  public void close() {
    closed = true;
    manager.removeSocket(this);
    super.close();
  }

  @Override
  public String toString() {
    return "LSocket{"+socket+"}";
  }
}
