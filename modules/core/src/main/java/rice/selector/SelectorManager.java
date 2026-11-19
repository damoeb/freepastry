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
package rice.selector;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import rice.Destructable;
import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.TimeSource;

/**
 * This class is the class which handles the selector, and listens for activity.
 * When activity occurs, it figures out who is interested in what has happened,
 * and hands off to that object.
 *
 * @version $Id: SelectorManager.java 4675 2009-03-11 15:54:34Z jeffh $
 * @author Alan Mislove
 */
public class SelectorManager extends Thread implements Timer, Destructable {

  // the maximal time to sleep on a select operation
  public static final int TIMEOUT = 500;

  // the underlying selector used
  protected Selector selector;

  // a list of the invocations that need to be done in this thread
  protected @Nullable List<Runnable> invocations;

  // the list of handlers which want to change their key
  protected HashSet<SelectionKey> modifyKeys;

  // the list of keys waiting to be cancelled
  private final HashSet<SelectionKey> cancelledKeys;

  // the set used to store the timer events
  protected Queue<TimerTask> timerQueue = new PriorityQueue<TimerTask>();

  // the next time the selector is scheduled to wake up
  private long wakeupTime = 0;

  protected TimeSource timeSource;

  long lastTime = 0;

  protected Logger logger;

  private final @Nullable String instance;

  // written by destroy() on any thread, read by the selector thread
  protected volatile boolean running = true;

  /**
   * Maximum number of rounds of invocations that are run after destroy(): invocations scheduled while draining
   * run in the next round. (An invocation that reschedules itself, such as a simulator loop, runs only once.)
   */
  private static final int MAX_SHUTDOWN_ROUNDS = 64;

  /**
   * Can be disabled for the simulator to improve performance, only do this
   * if you know you don't need to select on anything
   */
  private boolean select = true;

  private RandomSource random;

  /**
   * Constructor.
   */
  public SelectorManager(@Nullable String instance,
      TimeSource timeSource, LogManager log, @Nullable RandomSource random) {
    super(instance == null ? "FreePastry Selector" : "FreePastry Selector -- "
        + instance);
    this.random = random != null ? random : new SimpleRandomSource(log);
    this.instance = instance;
    this.logger = log.getLogger(getClass(), instance);
    this.invocations = new ArrayList<Runnable>();
    this.modifyKeys = new HashSet<SelectionKey>();
    this.cancelledKeys = new HashSet<SelectionKey>();
    this.timeSource = timeSource;

    try {
      selector = Selector.open();
    } catch (IOException e) {
      throw new UncheckedIOException("Error creating selector", e);
    }
    lastTime = timeSource.currentTimeMillis();
  }

  public SelectorManager(@Nullable String instance,
      TimeSource timeSource, LogManager log) {
    this(instance,timeSource,log,new SimpleRandomSource(log));
  }

  protected @Nullable Environment environment;

  /**
   * Can only be called once.  The rest of the time the argument is rejected.
   * This is needed so that when the NodeFactory clones the Environment, it doesn't
   * get restarted/have multiple different environments etc...
   *
   * @param env the environment; also starts this thread on the first call
   */
  public void setEnvironment(Environment env) {
    if (env == null) throw new IllegalArgumentException("env is null!");
    if (environment != null) return;
    environment = env;
    environment.addDestructable(this);
    start();
  }

  /**
   * Method which asks the Selector Manager to add the given key to the
   * cancelled set. If no one calls register on this key during the rest of this
   * select() operation, the key will be cancelled. Otherwise, it will be
   * returned as a result of the register operation.
   *
   * @param key The key to cancel
   */
  public void cancel(SelectionKey key) {
    if (key == null)
      throw new NullPointerException();

    synchronized (this) {
      if (invocations != null) {
        cancelledKeys.add(key);
        return;
      }
    }
    // already shut down, there is no next select() to cancel it in
    key.cancel();
  }

  /**
   * Registers a new channel with the selector, and attaches the given
   * SelectionKeyHandler as the handler for the newly created key. Operations
   * which the handler is interested in will be called as available.
   *
   * @param channel The channel to register with the selector
   * @param handler The handler to use for the callbacks
   * @param ops The initial interest operations
   * @return The SelectionKey which uniquely identifies this channel
   */
  public SelectionKey register(SelectableChannel channel,
      SelectionKeyHandler handler, int ops) throws IOException {
    if ((channel == null) || (handler == null))
      throw new NullPointerException();

    SelectionKey key = channel.register(selector, ops, handler);
    if (cancelledKeys != null) cancelledKeys.remove(key);

    return key;
  }

  /**
   * This method schedules a runnable task to be done by the selector thread
   * during the next select() call. All operations which modify the selector
   * should be done using this method, as they must be done in the selector
   * thread.
   *
   * @param d The runnable task to invoke
   */
  public synchronized void invoke(Runnable d) {
    if (d == null)
      throw new NullPointerException();
    if (invocations == null) {
      logger.debug("Dropping invocation {}, the selector was destroyed.", d);
      return;
    }
    invocations.add(d);
    wakeup();
  }

  /**
   * Debug method which returns the number of pending invocations
   *
   * @return The number of pending invocations
   */
  public synchronized int getNumInvocations() {
    return (invocations == null) ? 0 : invocations.size();
  }

  /**
   * Adds a selectionkey handler into the list of handlers which wish to change
   * their keys. Thus, modifyKeys() will be called on the next selection
   * operation
   *
   * @param key The key which is to be changed
   */
  public synchronized void modifyKey(SelectionKey key) {
    if (key == null)
      throw new NullPointerException();

    modifyKeys.add(key);
    wakeup();
  }

  /**
   * This method is to be implemented by a subclass to do some task each loop.
   */
  protected void onLoop() {
  }

  /**
   * This method starts the socket manager listening for events. It is designed
   * to be started when this thread's start() method is invoked.
   */
  @Override
  @SuppressWarnings("ThreadPriorityCheck") // the yield only lets other threads take the selector lock
  public void run() {
    logger.info("SelectorManager -- {} starting...", instance);

    lastTime = timeSource.currentTimeMillis();
    // loop while waiting for activity
    while (running) {
      try {
        if (useLoopListeners)
          notifyLoopListeners();

        // NOTE: This is so we aren't always holding the selector lock when we
        // get context switched
        Thread.yield();
        executeDueTasks();
        onLoop();
        doInvocations();
        if (select) {
          processSelectedKeys();
          int selectTime = SelectorManager.TIMEOUT;
          if (timerQueue.size() > 0) {
            TimerTask first = (TimerTask) timerQueue.peek();
            selectTime = (int) (first.scheduledExecutionTime() - timeSource
                .currentTimeMillis());
          }

          select(selectTime);

          if (cancelledKeys.size() > 0) {
            for (SelectionKey selectionKey : cancelledKeys)
              selectionKey.cancel();

            cancelledKeys.clear();

            // now, hack to make sure that all cancelled keys are actually
            // cancelled (dumb)
            selector.selectNow();
          }
        } // if select
      } catch (Throwable t) {
        logger.error("ERROR (SelectorManager.run): ", t);
        getEnvironment().getExceptionStrategy().handleException(this, t);
      }
    } // while(running)
    shutdown();
    logger.info("Selector {} shutting down.", instance);
  }

  /**
   * Called on the selector thread once the loop has stopped. Runs the invocations that are still pending,
   * because they typically release resources (such as Destructables queued by Environment.destroy(), or
   * transport layers closing their sockets), then drops the timer tasks, closes the channels of all keys
   * that are still registered (so that no bound port outlives the selector) and finally the selector itself.
   */
  protected void shutdown() {
    // 1. run the pending invocations, including those they schedule, in bounded rounds
    Set<Runnable> ran = Collections.newSetFromMap(new IdentityHashMap<Runnable, Boolean>());
    int skipped = 0;
    for (int round = 0; round < MAX_SHUTDOWN_ROUNDS; round++) {
      ArrayList<Runnable> pending;
      synchronized (this) {
        List<Runnable> current = pendingInvocations();
        if (current.isEmpty()) break;
        pending = new ArrayList<Runnable>(current);
        current.clear();
      }
      for (Runnable r : pending) {
        if (!ran.add(r)) {
          // it rescheduled itself (e.g. the simulator's event loop), don't keep it going
          skipped++;
          continue;
        }
        try {
          r.run();
        } catch (Throwable t) {
          // keep going: the rest must still run, and there is no loop left to report to
          logger.warn("Error running {} while destroying the selector", r, t);
        }
      }
    }

    int droppedInvocations;
    int droppedTasks;
    synchronized (this) {
      droppedInvocations = pendingInvocations().size();
      // from now on invoke() drops, and cancel() cancels immediately
      invocations = null;
      droppedTasks = timerQueue.size();
      timerQueue.clear();
      modifyKeys.clear();
      cancelledKeys.clear();
    }
    if (droppedInvocations > 0)
      logger.warn("Dropped {} invocations that were still being scheduled after {} rounds during shutdown.", droppedInvocations, MAX_SHUTDOWN_ROUNDS);
    if (skipped > 0 || droppedTasks > 0)
      logger.debug("Dropped {} rescheduled invocations and {} timer tasks during shutdown.", skipped, droppedTasks);
    synchronized (loopObservers) {
      loopObservers.clear();
    }

    // 2. close the channels of the keys that are still registered (sockets, server sockets, datagram channels)
    if (selector != null) {
      try {
        for (SelectionKey key : new ArrayList<SelectionKey>(selector.keys())) {
          try {
            key.channel().close();
          } catch (IOException ioe) {
            logger.atWarn().setCause(ioe).log(() -> "Error closing "+key.channel()+" while destroying the selector");
          }
          key.cancel();
        }
      } catch (ClosedSelectorException ignored) {
        // already closed
      }

      // 3. the selector itself
      try {
        selector.close();
      } catch (IOException ioe) {
        logger.warn("Error cancelling selector:", ioe);
      }
    }
  }

  /**
   * Stops the selector. This may be called on any thread, also by an invocation on the selector thread.
   * The selector thread finishes its current loop, runs the invocations that are still pending, and closes
   * all channels that are still registered, see {@link #shutdown()}. Join this thread to wait for that.
   */
  @Override
  public void destroy() {
    if (logger.isDebugEnabled()) {
      logger.debug("destroying SelectorManager", new Exception("Stack Trace"));
    } else logger.info("destroying SelectorManager");
    running = false;
    if (getState() == Thread.State.NEW) {
      // the loop never ran, so shutdown() won't either: release the selector here
      if (selector != null) {
        try {
          selector.close();
        } catch (IOException ioe) {
          logger.warn("Error closing selector:", ioe);
        }
      }
      return;
    }
    // don't wait for the select() timeout; Selector.wakeup() is thread safe, and sticky if not selecting yet
    if (selector != null) selector.wakeup();
  }

  /**
   * LoopListeners is used in case you are worried that your process may not get scheduled for a while
   * such as on an overloaded planetlab node, or if you hibernate your laptop, this is not needed
   * for the simulator
   */
  private boolean useLoopListeners = true;

  /**
   * Set this to false when using the simulator, because you don't need to notify loop observers.
   * @param val true to notify the loop observers on each loop
   */
  public void useLoopListeners(boolean val) {
    useLoopListeners = val;
  }

  protected void notifyLoopListeners() {
    long now = timeSource.currentTimeMillis();
    long diff = now - lastTime;
    // notify observers
    synchronized (loopObservers) {
      for (LoopObserver lo : loopObservers) {
        if (lo.delayInterest() <= diff) {
          lo.loopTime((int) diff);
        }
      }
    }
    lastTime = now;
  }

  final ArrayList<LoopObserver> loopObservers = new ArrayList<LoopObserver>();

  public void addLoopObserver(LoopObserver lo) {
    synchronized (loopObservers) {
      loopObservers.add(lo);
    }
  }

  public void removeLoopObserver(LoopObserver lo) {
    synchronized (loopObservers) {
      loopObservers.remove(lo);
    }
  }

  protected void processSelectedKeys() throws IOException {
    SelectionKey[] keys = selectedKeys();

    // to debug weird selection bug
    if (keys.length > 1000 && logger.isDebugEnabled()) {
      logger.debug("lots of selection keys!");
      HashMap<String, Integer> histo = new HashMap<String, Integer>();
      for (SelectionKey key : keys) {
        String keyclass = key.getClass().getName();
        if (histo.containsKey(keyclass)) {
          histo.put(keyclass, Integer.valueOf(((Integer)histo.get(keyclass)).intValue() + 1));
        } else {
          histo.put(keyclass, Integer.valueOf(1));
        }
      }
      logger.debug("begin selection keys by class");
      for (String name : histo.keySet()) {
        logger.debug("Selection Key: " + name + ": "+histo.get(name));
      }
      logger.debug("end selection keys by class");
    }

    for (SelectionKey key : keys) {
      selector.selectedKeys().remove(key);

      synchronized (key) {
        SelectionKeyHandler skh = (SelectionKeyHandler) key.attachment();

        if (skh != null) {
          // accept
          if (key.isValid() && key.isAcceptable()) {
            skh.accept(key);
          }

          // connect
          if (key.isValid() && key.isConnectable()) {
            skh.connect(key);
          }

          // read
          if (key.isValid() && key.isReadable()) {
            skh.read(key);
          }

          // write
          if (key.isValid() && key.isWritable()) {
            skh.write(key);
          }
        } else {
          key.channel().close();
          key.cancel();
        }
      }
    }
  }

  /**
   * Method which invokes all pending invocations. This method should *only* be
   * called by the selector thread.
   */
  protected void doInvocations() {
    logger.trace("SM.doInvocations()");
    Iterator<Runnable> i;
    synchronized (this) {
      List<Runnable> current = pendingInvocations();
      i = new ArrayList<Runnable>(current).iterator();
      current.clear();
    }

    while (i.hasNext()) {
      Runnable run = (Runnable) i.next();
      try {
        run.run();
      } catch (RuntimeException e) {
        // place the rest of the items back into invocations
        synchronized(this) {
          int ctr = 0;
          while(i.hasNext()) {
            pendingInvocations().add(ctr,i.next());
            ctr++;
          }
        }
        throw e;
      }
    }

    Iterator<SelectionKey> i2;
    synchronized (this) {
      i2 = new ArrayList<SelectionKey>(modifyKeys).iterator();
      modifyKeys.clear();
    }

    while (i2.hasNext()) {
      SelectionKey key = (SelectionKey) i2.next();
      if (key.isValid() && (key.attachment() != null))
        ((SelectionKeyHandler) key.attachment()).modifyKey(key);
    }
  }

  /**
   * Selects on the selector, and returns the result. Selects without blocking if
   * time is not positive or invocations or key modifications are pending.
   *
   * @param time the maximum time to wait in millis, capped at TIMEOUT
   * @return the number of selected keys (1 after an interrupted system call)
   * @throws IOException if the select fails
   */
  protected int select(int time) throws IOException {
    logger.trace("SM.select({})", time);

    if (time > TIMEOUT)
      time = TIMEOUT;

    try {
      if ((time <= 0) || (pendingInvocations().size() > 0) || (modifyKeys.size() > 0))
        return selector.selectNow();

      wakeupTime = timeSource.currentTimeMillis() + time;
      return selector.select(time);
    } catch (CancelledKeyException cce) {
      logger.warn("CCE: cause:", cce.getCause());
      throw cce;
    } catch (IOException e) {
      String message = e.getMessage();
      if (message != null && message.contains("Interrupted system call")) {
        logger.warn("Got interrupted system call, continuing anyway...");
        return 1;
      } else {
        throw e;
      }
    }
  }

  /**
   * Returns the currently selected keys of the selector as an array of keys,
   * in random order.
   *
   * @return The array of keys
   */
  protected SelectionKey[] selectedKeys() throws IOException {
    Set<SelectionKey> s = selector.selectedKeys();
    SelectionKey[] k = (SelectionKey[])s.toArray(new SelectionKey[0]);
    // randomize k
    for (int c = 0; c < k.length; c++) {
      SelectionKey temp = k[c];
      int swapIndex = random.nextInt(k.length);
      k[c] = k[swapIndex];
      k[swapIndex] = temp;
    }

    return k;
  }

  /**
   * Returns whether or not this thread of execution is the selector thread
   *
   * @return Whether or not this is the selector thread
   */
  @SuppressWarnings("ReferenceEquality") // this manager is the selector thread
  public boolean isSelectorThread() {
    return (Thread.currentThread() == this);
  }

  /**
   * Method which schedules a task to run at its own next execution time, which must already be set
   *
   * @param task The task to run
   * @return the task
   */
  @Override
  public TimerTask schedule(TimerTask task) {
    addTask(task);
    return task;
  }

  @Override
  public TimerTask schedule(TimerTask task, long delay) {
    task.setNextExecutionTime(timeSource.currentTimeMillis() + delay);
    addTask(task);
    return task;
  }

  /**
   * Method which schedules a task to run repeatedly after a specified delay and
   * period
   *
   * @param task The task to run
   * @param delay The delay before first running, in milliseconds
   * @param period The period with which to run in milliseconds
   */
  @Override
  public TimerTask schedule(TimerTask task, long delay, long period) {
    task.setNextExecutionTime(timeSource.currentTimeMillis() + delay);
    task.period = (int) period;
    addTask(task);
    return task;
  }

  /**
   * Method which schedules a task to run repeatedly (at a fixed rate) after a
   * specified delay and period
   *
   * @param task The task to run
   * @param delay The delay before first running in milliseconds
   * @param period The period with which to run in milliseconds
   */
  @Override
  public TimerTask scheduleAtFixedRate(TimerTask task, long delay, long period) {
    task.setNextExecutionTime(timeSource.currentTimeMillis() + delay);
    task.period = (int) period;
    task.fixedRate = true;
    addTask(task);
    return task;
  }

  private final Object seqLock = new Object();
  private int seqCtr = Integer.MIN_VALUE;

  /**
   * Internal method which adds a task to the task queue, waking up the selector
   * if necessary to recalculate the sleep time
   *
   * @param task The task to add
   */
  protected synchronized void addTask(TimerTask task) {
    if (invocations == null) {
      // destroyed, it would never run
      logger.debug("Dropping task {}, the selector was destroyed.", task);
      return;
    }
    synchronized(seqLock) {
      task.seq = seqCtr++;
    }
    logger.atTrace().log(() -> "addTask("+task+") scheduled for "+task.scheduledExecutionTime());
      if (!timerQueue.add(task)) {
        logger.warn("ERROR: Got false while enqueueing task {}!", task);
        Thread.dumpStack();
      } else {
        task.setSelectorManager(this);
      }

    // need to interrupt thread if waiting too long in selector
    if (select) {
      // using the network
      if (wakeupTime >= task.scheduledExecutionTime()) {
        // we need to wake up the selector because it's going to sleep too long
        wakeup();
      }
    } else {
      // using the simulator
      if (task.scheduledExecutionTime() == getNextTaskExecutionTime()) {
        // we need to wake up the selector because we are now the newest
        // shortest wait, and may be delaying because of a later event
        wakeup();
      }
    }
  }

  public synchronized void removeTask(TimerTask task) {
    logger.atTrace().log(() -> "removeTask("+task+") scheduled for "+task.scheduledExecutionTime());
    timerQueue.remove(task);
  }

  /**
   * Note, should hold the selector's (this) lock to call this.
   */
  public void wakeup() {
    selector.wakeup();
    this.notifyAll();
  }

  public long getNextTaskExecutionTime() {
    if (timerQueue.size() > 0) {
      TimerTask next = (TimerTask) timerQueue.peek();
      return next.scheduledExecutionTime();
    }
    return -1;
  }

  /**
   * Internal method which finds all due tasks and executes them.
   */
  protected void executeDueTasks() {
    assert isSelectorThread() : "timers run on the selector thread";
    long now = timeSource.currentTimeMillis();
    logger.trace("SM.executeDueTasks() {}", now);

    ArrayList<TimerTask> executeNow = new ArrayList<TimerTask>();

    // step 1, fetch all due timers
    synchronized (this) {
      boolean done = false;
      while (!done) {
        if (timerQueue.size() > 0) {
          TimerTask next = (TimerTask) timerQueue.peek();
          if (next.scheduledExecutionTime() <= now) {
            executeNow.add(next);
            timerQueue.poll();
          } else {
            done = true;
          }
        } else {
          done = true;
        }
      }
    }

    // step 2, execute them all
    // items to be added back into the queue
    ArrayList<TimerTask> addBack = new ArrayList<TimerTask>();
    Iterator<TimerTask> i = executeNow.iterator();
    while (i.hasNext()) {
      TimerTask next = i.next();
      try {
        logger.trace("executing task {}", next);

        if (executeTask(next)) {
          addBack.add(next);
        }
      } catch (RuntimeException e) {
        // add back the un-executed tasks before continuing
        while(i.hasNext()) {
          addBack.add(i.next());
        }
        synchronized (this) {
          i = addBack.iterator();
          while (i.hasNext()) {
            TimerTask tt = (TimerTask) i.next();
            timerQueue.add(tt);
          }
        }

        throw e;
      }
    }

    // step 3, add them back if necessary
    synchronized (this) {
      i = addBack.iterator();
      while (i.hasNext()) {
        TimerTask tt = (TimerTask) i.next();
        timerQueue.add(tt);
      }
    }
  }

  protected boolean executeTask(TimerTask next) {
    return next.execute(timeSource);
  }

  /**
   * Returns the timer associated with this SelectorManager (in this case, it is
   * this).
   *
   * @return The associated timer
   */
  public Timer getTimer() {
    return this;
  }

  public Selector getSelector() {
    return selector;
  }

  public void setSelect(boolean b) {
    select = b;
  }

  public Environment getEnvironment() {
    return Objects.requireNonNull(environment, "setEnvironment() was not called");
  }

  /**
   * Returns the pending invocations; only valid until {@link #shutdown()} drops them.
   */
  protected List<Runnable> pendingInvocations() {
    return Objects.requireNonNull(invocations, "the selector has shut down");
  }


  public static void main(String[] args) {
    new Environment();
    new Environment();
  }

  public RandomSource getRandom() {
    return random;
  }
}
