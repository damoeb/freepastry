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
package org.mpisws.p2p.transport.peerreview.replay.record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.mpisws.p2p.transport.peerreview.replay.DeterministicSM;
import rice.environment.logging.LogManager;
import rice.environment.random.RandomSource;
import rice.environment.time.TimeSource;
import rice.environment.time.simulated.DirectTimeSource;

/**
 * This is the SelectorManager for PeerReview.  The invariant here is that we use a simTime that isn't updated near as
 * frequently as the real clock.  This makes the events more discrete for replay.
 * <p>
 * An invocation from another thread (e.g. a message delivered by an in-memory transport, or a processor's result)
 * is an input to the node, like a message read in the select phase: the replay delivers its logged event after
 * every timer due at or before that event. Such an invocation therefore runs only after the due timers, at the
 * current time, each one as its own step of {@link #advanceTo}. The invocations of the selector thread itself run in
 * the rounds of {@link #advanceTo}, as in the replay.
 *
 * @author Jeff Hoye
 *
 */
public class RecordSM extends DeterministicSM {
  TimeSource realTime;

  public RecordSM(String instance, TimeSource realTime, DirectTimeSource simTime, LogManager log, RandomSource rs) {
    super(instance, simTime, log, rs);
    this.realTime = realTime;
  }

  /** An invocation from another thread: an input to the node. */
  private record Input(Runnable task) implements Runnable {
    @Override
    public void run() {
      task.run();
    }
  }

  @Override
  public synchronized void invoke(Runnable d) {
    Objects.requireNonNull(d);
    super.invoke(isSelectorThread() ? d : new Input(d));
  }

  @Override
  protected void executeDueTasks() {
    long now = realTime.currentTimeMillis();
    do {
      advanceTo(() -> now);
    } while (runNextInput());
  }

  /**
   * Runs the oldest pending invocation from another thread.
   *
   * @return false if there was none
   */
  private boolean runNextInput() {
    Runnable input = null;
    synchronized (this) {
      for (Iterator<Runnable> i = pendingInvocations().iterator(); i.hasNext();) {
        Runnable r = i.next();
        if (r instanceof Input) {
          i.remove();
          input = r;
          break;
        }
      }
    }
    if (input == null) return false;
    logger.debug("running input {}", input);
    input.run();
    return true;
  }

  /**
   * A round of the invocations of the selector thread; the inputs wait for {@link #runNextInput()}.
   */
  @Override
  protected void runInvocations() {
    List<Runnable> inputs = new ArrayList<Runnable>();
    synchronized (this) {
      for (Iterator<Runnable> i = pendingInvocations().iterator(); i.hasNext();) {
        Runnable r = i.next();
        if (r instanceof Input) {
          inputs.add(r);
          i.remove();
        }
      }
    }
    try {
      super.runInvocations();
    } finally {
      if (!inputs.isEmpty()) {
        synchronized (this) {
          // ahead of the inputs that arrived meanwhile
          List<Runnable> pending = pendingInvocations();
          int first = 0;
          while (first < pending.size() && !(pending.get(first) instanceof Input)) first++;
          pending.addAll(first, inputs);
        }
      }
    }
  }
}
