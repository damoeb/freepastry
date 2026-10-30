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
/*
 * Created on Nov 8, 2005
 */
package rice.environment.time.simulated;

import java.util.HashSet;

import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.environment.EnvironmentConfig;
import rice.environment.logging.LogManager;
import rice.environment.params.Parameters;
import rice.environment.time.TimeSource;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;

@SuppressWarnings("SynchronizeOnNonFinalField") // selectorManager is set once by setSelectorManager, before anyone sleeps
public class DirectTimeSource implements TimeSource {

  private long time = 0;
  // replaced by setLogManager(); the default lets the clock be used on its own
  private Logger logger = LoggerFactory.getLogger(DirectTimeSource.class);
  private final @Nullable String instance;
  private @Nullable SelectorManager selectorManager;

  /**
   * When destroy is called, throw an interrupted exception on all of these.
   */
  private final HashSet<BlockingTimerTask> pendingTimers = new HashSet<BlockingTimerTask>();

  public DirectTimeSource(long time) {
    this(time, null);
  }

  public DirectTimeSource(long time, @Nullable String instance) {
    if (time < 0) {
      this.time = System.currentTimeMillis();
    } else {
      this.time = time;
    }
    this.instance = instance;
  }

  public DirectTimeSource(Parameters p) {
    this(EnvironmentConfig.fromParameters(p).directSimulatorStartTime());
  }

  public void setLogManager(LogManager manager) {
    logger = manager.getLogger(DirectTimeSource.class, instance);
  }

  private SelectorManager selectorManager() {
    return Objects.requireNonNull(selectorManager, "call setSelectorManager() first");
  }

  public void setSelectorManager(SelectorManager sm) {
    selectorManager = sm;

  }

  @Override
  public long currentTimeMillis() {
    return time;
  }

  /**
   * Should be synchronized on the selectorManager
   */
  public void setTime(long newTime) {
    if (newTime < time) {
      logger.warn("Attempted to set time from {} to {}, ignoring.", time, newTime);
      return;
    }
    if (newTime > time) logger.trace("DirectTimeSource.setTime({}=>{})", time, newTime);
    time = newTime;
  }

  /**
   * Should be synchronized on the selectorManager
   */
  public void incrementTime(int millis) {
    setTime(time+millis);
  }

  private class BlockingTimerTask extends TimerTask {
    boolean done = false;
    boolean interrupted = false;

    @Override
    public void run() {
      SelectorManager sm = selectorManager();
      synchronized(sm) {
        done = true;
        sm.notifyAll();
        // no Thread.yield() needed: the selector already yields enough
      }
    }

    void interrupt() {
      interrupted = true;
    }

  }

  @Override
  public void sleep(long delay) throws InterruptedException {
    SelectorManager sm = selectorManager();
    synchronized(sm) { // to prevent an out of order acquisition
      // we only lock on the selector
      BlockingTimerTask btt = new BlockingTimerTask();
      pendingTimers.add(btt);
      logger.debug("DirectTimeSource.sleep({})", delay);

      sm.getTimer().schedule(btt,delay);

      while(!btt.done) {
        sm.wait();
        if (btt.interrupted) throw new InterruptedException("TimeSource destroyed.");
      }
      pendingTimers.remove(btt);
    }
  }

  /**
   * Wakes up the threads blocked in {@link #sleep(long)}, they throw an InterruptedException.
   */
  @Override
  public void destroy() {
    if (selectorManager == null) return; // nobody can be sleeping
    synchronized(selectorManager) {
      for (BlockingTimerTask btt : pendingTimers) {
        btt.interrupt();
      }
      pendingTimers.clear();
      selectorManager.notifyAll();
    }
  }

}
