package org.mpisws.p2p.transport.priority;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import rice.p2p.commonapi.Cancellable;

/** A {@link ReadScheduler} with a manual clock: tests run the selector passes and move the time themselves. */
final class ManualReadScheduler implements ReadScheduler {
  private long nowMillis;
  private List<Runnable> nextPass = new ArrayList<>();
  private final List<Timer> timers = new ArrayList<>();

  private static final class Timer implements Cancellable {
    final long dueMillis;
    final Runnable task;
    boolean isCancelled;

    Timer(long dueMillis, Runnable task) {
      this.dueMillis = dueMillis;
      this.task = task;
    }

    @Override
    public boolean cancel() {
      boolean wasPending = !isCancelled;
      isCancelled = true;
      return wasPending;
    }
  }

  ManualReadScheduler(long startMillis) {
    this.nowMillis = startMillis;
  }

  @Override
  public long nowMillis() {
    return nowMillis;
  }

  @Override
  public void nextPass(Runnable task) {
    nextPass.add(task);
  }

  @Override
  public Cancellable after(long delayMillis, Runnable task) {
    Timer timer = new Timer(nowMillis + delayMillis, task);
    timers.add(timer);
    return timer;
  }

  /** Runs the tasks waiting for the next pass (not those they schedule in turn). */
  void startPass() {
    List<Runnable> tasks = nextPass;
    nextPass = new ArrayList<>();
    tasks.forEach(Runnable::run);
  }

  /** Moves the clock and runs the timers that are due, in their order. */
  void advance(long millis) {
    nowMillis += millis;
    while (true) {
      Timer due = timers.stream().filter(timer -> timer.dueMillis <= nowMillis)
          .min(Comparator.comparingLong(timer -> timer.dueMillis)).orElse(null);
      if (due == null) return;
      timers.remove(due);
      if (!due.isCancelled) due.task.run();
    }
  }

  /** The timers that will still run. */
  long pendingTimers() {
    return timers.stream().filter(timer -> !timer.isCancelled).count();
  }
}
