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
package org.mpisws.p2p.transport.peerreview.replay.playback;

import org.mpisws.p2p.transport.peerreview.replay.DeterministicSM;
import org.mpisws.p2p.transport.peerreview.replay.Verifier;
import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.time.simulated.DirectTimeSource;

/**
 * There are normally 3 kinds of events:
 *   Invokations
 *   TimerTasks
 *   Network I/O
 *
 * The Network I/O should match exactly with our Log, and so we only have to pump Invokations
 *
 * @author Jeff Hoye
 *
 */
public class ReplaySM extends DeterministicSM {
  Verifier<?, ?> verifier;

  public ReplaySM(String instance, DirectTimeSource timeSource, LogManager log) {
    super(instance, timeSource, log, null);
    setSelect(false);
  }

  public void setVerifier(Verifier<?, ?> v) {
    this.verifier = v;
  }

  /**
   * Don't automatically start the thread.
   */
  @Override
  public void setEnvironment(Environment env) {
    if (env == null) throw new IllegalArgumentException("env is null!");
    if (environment != null) return;
    environment = env;
  }

  public boolean makeProgress() {
    // Handle any pending timers. Note that we have to be sure to call them in the exact same
    // order as in the main code (RecordSM); otherwise there can be subtle bugs and side-effects.
    logger.trace("executeDueTasks()");

    if (isSuccess()) return false;
    if (!advanceTo(verifier::getNextEventTime, verifier::isNextEventBeforeTimersAtItsTime)) return false;

    return verifier.makeProgress();
  }

  protected boolean isSuccess() {
    boolean ret = verifier.isSuccess();
    if (ret) {
      // TODO: let the caller decide what happens on success instead of destroying the environment
      environment.destroy();
    }
    return ret;
  }
}
