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
package org.mpisws.p2p.transport.peerreview.replay;

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import rice.environment.logging.LogManager;
import rice.environment.random.RandomSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.selector.SelectorLoop;
import rice.selector.TimerTask;

/**
 * The SelectorLoop of a node whose log PeerReview replays: the {@link org.mpisws.p2p.transport.peerreview.replay.record.RecordSM}
 * while the node runs, and the {@link org.mpisws.p2p.transport.peerreview.replay.playback.ReplaySM} while an auditor
 * replays its log. Both run the timers and the invocations in the order of
 * {@link #advanceTo(LongSupplier, BooleanSupplier)}, before each input event (a message, a socket event, a
 * checkpoint, ...), so the replay reproduces the order of the recording.
 *
 * @see #advanceTo(LongSupplier, BooleanSupplier)
 */
public abstract class DeterministicSM extends SelectorLoop {
  /** The time of a log without a next event. */
  public static final long NO_EVENT = -1;

  protected final DirectTimeSource simTime;

  protected DeterministicSM(String instance, DirectTimeSource simTime, LogManager log, RandomSource rs) {
    super(instance, simTime, log, rs);
    this.simTime = simTime;
  }

  @Override
  protected synchronized void addTask(TimerTask task) {
    long now = timeSource.currentTimeMillis();
    if (task.scheduledExecutionTime() < now) {
      logger.atWarn().setCause(new Exception("Stack Trace")).log(() -> "Can't schedule a task in the past. "+task+" now:"+now+" task.execTime:"+task.scheduledExecutionTime());
      throw new RuntimeException("Can't schedule a task in the past.");
    }
    super.addTask(task);
  }

  /**
   * Brings the node up to its next input event, which happens at {@code eventTime}: runs every timer due at or
   * before that time, in the order of their times and each at its own time, after a round of the pending
   * invocations; then sets the clock to the event's time and runs another round. The input event comes next.
   * <p>
   * The replay predicts the outputs (e.g. a SEND) of the timers and invocations from the log, so it knows the time of
   * the next input event only after them: {@code eventTime} is asked again after each timer.
   * <p>
   * An event that a timer of the record logs itself (a CHECKPOINT) comes before the timers due in the same
   * millisecond that the record ran after it: with {@code beforeTimersAtEventTime}, only the timers due strictly
   * before the event's time run. A timer due at that time that the record ran first has logged its output (e.g. a
   * SEND) before the event, and that output is the next event until the timer has run.
   *
   * @param eventTime the time of the next input event, or {@link #NO_EVENT}
   * @param beforeTimersAtEventTime whether the next input event comes before the timers due at its time
   * @return false if there is no next event; the clock and the pending invocations are then left as they are
   */
  protected boolean advanceTo(LongSupplier eventTime, BooleanSupplier beforeTimersAtEventTime) {
    long time;
    while (true) {
      time = eventTime.getAsLong();
      if (time == NO_EVENT) return false;
      long lastDue = beforeTimersAtEventTime.getAsBoolean() ? time - 1 : time;

      TimerTask next;
      synchronized (this) {
        next = timerQueue.peek();
        if (next == null || next.scheduledExecutionTime() > lastDue) break;
        timerQueue.poll();
        simTime.setTime(next.scheduledExecutionTime());
      }

      runInvocations();
      logger.debug("executing task {}", next);
      if (next.execute(simTime)) {
        synchronized (this) {
          timerQueue.add(next); // reschedule a periodic task
        }
      }
    }
    simTime.setTime(time);
    runInvocations();
    return true;
  }

  /**
   * Runs one round of the invocations the node queued itself: those pending now. The invocations they queue run in
   * the next round.
   */
  protected void runInvocations() {
    super.doInvocations();
  }

  @Override
  protected void doInvocations() {
    // do nothing, the invocations run in advanceTo()
  }
}
