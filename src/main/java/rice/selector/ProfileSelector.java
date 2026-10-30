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
 * Created on Jul 27, 2004
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package rice.selector;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import rice.environment.logging.LogManager;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.TimeSource;

/**
 * @author jeffh
 *
 * To change the template for this generated type comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class ProfileSelector extends SelectorManager {
  public static boolean useHeartbeat = true;
  int HEART_BEAT_INTERVAL = 60000;
  long lastHeartBeat = 0;

  public static boolean recordStats = true;

  public String lastTaskType = null;
  public String lastTaskClass = null;
  public String lastTaskToString = null;
  public long lastTaskHash = 0;

  int numInvocationsScheduled = 0;
  int numInvocationsExecuted = 0;

  public ProfileSelector(String instance,
      TimeSource timeSource, LogManager log) {
    this(instance,timeSource,log,null);
  }

  public ProfileSelector(String instance, TimeSource timeSource, LogManager log, RandomSource rand) {
    this(instance, timeSource,log, rand, 60000);
  }

  /**
   * Also starts a watchdog thread that prints the last task every lastTaskTime millis.
   */
  public ProfileSelector(String instance, TimeSource timeSource, LogManager log, RandomSource rand, final int lastTaskTime) {
    super(instance, timeSource, log, (rand == null)?new SimpleRandomSource(log):rand);
    new Thread(() -> {
      while(true) {
        System.out.println("LastTask: type:"+lastTaskType+" class:"+lastTaskClass+" toString():"+lastTaskToString+" hash:"+lastTaskHash);
        try {
          Thread.sleep(lastTaskTime);
        } catch (InterruptedException ignored) {
          // this watchdog runs until the JVM exits, so just keep printing
        }
      }
    }, "ProfileSelectorWatchdog").start();

  }

  protected void onLoop() {
    if (!useHeartbeat) return;
    long curTime = timeSource.currentTimeMillis();
    if ((curTime - lastHeartBeat) > HEART_BEAT_INTERVAL) {
      System.out.println("selector heartbeat "+new Date()+" maxInvokes:"+maxInvokes+" invokesSched:"+numInvocationsScheduled+" invokesExe:"+numInvocationsExecuted+" CurrentThread:"+Thread.currentThread()+"@"+System.identityHashCode(Thread.currentThread()));
      printStats();
      lastHeartBeat = curTime;
    }
  }

  int maxInvokes = 0;
  public void invoke(Runnable d) {
    synchronized(this) {
      numInvocationsScheduled++;
      super.invoke(d);
    }
    int numInvokes = getNumInvocations(); // null-safe after destroy()
    if (numInvokes > maxInvokes) {
      maxInvokes = numInvokes;
    }
  }

  // *********************** debugging statistics ****************
  /**
   * Records how long it takes to receive each type of message.
   * Written by the selector thread, but printStats() may be called from any thread, so it must be concurrent.
   */
  private final ConcurrentHashMap<String, Stat> stats = new ConcurrentHashMap<String, Stat>();

  public void addStat(String s, long time) {
    if (!recordStats) return;
    stats.computeIfAbsent(s, Stat::new).addTime(time);
  }

  public void printStats() {
    if (!recordStats) return;

    // a weakly consistent snapshot, no lock needed
    ArrayList<Stat> list = new ArrayList<Stat>(stats.values());

    Collections.sort(list,(arg0, arg1) -> {
      Stat stat1 = (Stat)arg0;
      Stat stat2 = (Stat)arg1;

      return Long.compare(stat2.totalTime, stat1.totalTime);
    });
    for (Stat stat : list) {
      System.out.println("  "+stat);
    }
  }

  protected boolean executeTask(TimerTask next) {
    lastTaskType = "Executing "+next;
    long startTime = timeSource.currentTimeMillis();
    boolean ret = super.executeTask(next);
    int time = (int)(timeSource.currentTimeMillis() - startTime);
    lastTaskType = "Executing "+next+" Complete";
    addStat("executing "+next.getClass(),time);
    return ret;
  }

  protected void doSelections() throws IOException {
    SelectionKey[] keys = selectedKeys();

    for (SelectionKey key : keys) {
      selector.selectedKeys().remove(key);

      SelectionKeyHandler skh = (SelectionKeyHandler) key.attachment();

      if (skh != null) {
        // accept
        if (key.isValid() && key.isAcceptable()) {
          lastTaskType = "Accept";
          lastTaskClass = skh.getClass().getName();
          lastTaskToString = skh.toString();
          lastTaskHash = System.identityHashCode(skh);
          long startTime = timeSource.currentTimeMillis();
          skh.accept(key);
          int time = (int)(timeSource.currentTimeMillis() - startTime);
          lastTaskType = "Accept Complete";
          addStat("accepting",time);
        }

        // connect
        if (key.isValid() && key.isConnectable()) {
          lastTaskType = "Connect";
          lastTaskClass = skh.getClass().getName();
          lastTaskToString = skh.toString();
          lastTaskHash = System.identityHashCode(skh);
          long startTime = timeSource.currentTimeMillis();
          skh.connect(key);
          int time = (int)(timeSource.currentTimeMillis() - startTime);
          lastTaskType = "Connect Complete";
          addStat("connecting",time);
        }

        // read
        if (key.isValid() && key.isReadable()) {
          lastTaskType = "Read";
          lastTaskClass = skh.getClass().getName();
          lastTaskToString = skh.toString();
          lastTaskHash = System.identityHashCode(skh);
          long startTime = timeSource.currentTimeMillis();
          skh.read(key);
          int time = (int)(timeSource.currentTimeMillis() - startTime);
          lastTaskType = "Read Complete";
        }

        // write
        if (key.isValid() && key.isWritable()) {
          lastTaskType = "Write";
          lastTaskClass = skh.getClass().getName();
          lastTaskToString = skh.toString();
          lastTaskHash = System.identityHashCode(skh);
          long startTime = timeSource.currentTimeMillis();
          skh.write(key);
          int time = (int)(timeSource.currentTimeMillis() - startTime);
          lastTaskType = "Write Complete";
        }
      } else {
        key.channel().close();
        key.cancel();
      }
    }
  }


  /**
   * Method which invokes all pending invocations. This method should *only* be
   * called by the selector thread.
   */
  protected void doInvocations() {
    Iterator<Runnable> i;
    synchronized(this) {
      i = new ArrayList<Runnable>(invocations).iterator();
      invocations.clear();
    }
    Runnable run;
    while (i.hasNext()) {
      numInvocationsExecuted++;
      run = (Runnable)i.next();
      try {
        lastTaskType = "Invocation";
        lastTaskClass = run.getClass().getName();
        lastTaskToString = run.toString();
        lastTaskHash = System.identityHashCode(run);
        long startTime = timeSource.currentTimeMillis();
        run.run();
        int time = (int)(timeSource.currentTimeMillis() - startTime);
        lastTaskType = "Invocation Complete";
      } catch (Exception e) {
        logger.error("Invoking runnable caused exception {} - continuing", e, e);
      }
    }

    Iterator<SelectionKey> i2;
    synchronized(this) {
      i2 = new ArrayList<SelectionKey>(modifyKeys).iterator();
      modifyKeys.clear();
    }
    SelectionKey key;
    while (i2.hasNext()) {
      key = (SelectionKey)i2.next();
      if (key.isValid() && (key.attachment() != null)) {
        SelectionKeyHandler skh = (SelectionKeyHandler) key.attachment();
        lastTaskType = "ModifyKey";
        lastTaskClass = skh.getClass().getName();
        lastTaskHash = System.identityHashCode(skh);
        lastTaskToString = skh.toString();
        skh.modifyKey(key);
        lastTaskType = "ModifyKey Complete";
      }
    }
  }

  protected void doInvocations2() {
    Runnable run = getInvocation();

    while (run != null) {
      try {
        lastTaskType = "Invocation";
        lastTaskClass = run.getClass().getName();
        lastTaskToString = run.toString();
        lastTaskHash = System.identityHashCode(run);
        long startTime = timeSource.currentTimeMillis();
        run.run();
        int time = (int)(timeSource.currentTimeMillis() - startTime);
        addStat(run.getClass().getName(),time);
        lastTaskType = "Invocation Complete";
      } catch (Exception e) {
        logger.error("Invoking runnable caused exception {} - continuing", e, e);
      }

      run = getInvocation();
    }

    SelectionKey key = getModifyKey();
    while (key != null) {
      if (key.isValid() && (key.attachment() != null)) {
        SelectionKeyHandler skh = (SelectionKeyHandler) key.attachment();
        lastTaskType = "ModifyKey";
        lastTaskClass = skh.getClass().getName();
        lastTaskHash = System.identityHashCode(skh);
        lastTaskToString = skh.toString();
        skh.modifyKey(key);
        lastTaskType = "ModifyKey Complete";
      }

      key = getModifyKey();
    }
  }



  /**
   * A statistic as to how long user code is taking to process a particular message.
   *
   * @author Jeff Hoye
   */
  class Stat {
    int num = 0;
    String name = null;
    long totalTime = 0;
    long maxTime = 0;

    public Stat(String name) {
      this.name = name;
    }

    public void addTime(long t) {
      num++;
      totalTime+=t;
      if (t > maxTime) {
        maxTime = t;
      }
    }

    public String toString() {
      long avgTime = totalTime/num;
      return name+"\t maxTime:"+maxTime+"\t avgTime:"+avgTime+"\t numInstances:"+num+"\t totalTime:"+totalTime;
    }
  }




}
