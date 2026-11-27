package org.mpisws.p2p.transport.priority;

import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Cancellable;
import rice.selector.SelectorLoop;
import rice.selector.TimerTask;

/**
 * The time and the timers that the budgets of the priority layer need: the environment's clock, a task for the next
 * selector pass, and a task after a delay. Tests use a manual implementation; everything runs on the selector thread.
 */
interface ReadScheduler {
  /** The current time of the environment's {@code TimeSource}. */
  long nowMillis();

  /** Runs the task on the selector thread, in the next pass of the selector loop. */
  void nextPass(Runnable task);

  /** Runs the task on the selector thread after the delay; cancelling the result stops it. */
  Cancellable after(long delayMillis, Runnable task);

  /** The scheduler of a selector, with the environment's time source. */
  static ReadScheduler of(SelectorLoop selector, TimeSource timeSource) {
    return new ReadScheduler() {
      @Override
      public long nowMillis() {
        return timeSource.currentTimeMillis();
      }

      @Override
      public void nextPass(Runnable task) {
        selector.invoke(task);
      }

      @Override
      public Cancellable after(long delayMillis, Runnable task) {
        return selector.schedule(new TimerTask() {
          @Override
          public void run() {
            task.run();
          }
        }, delayMillis);
      }
    };
  }

  /** A scheduler for budgets that never wait; any timer or pass task fails. */
  ReadScheduler NONE = new ReadScheduler() {
    @Override
    public long nowMillis() {
      return 0;
    }

    @Override
    public void nextPass(Runnable task) {
      throw new IllegalStateException("an unlimited budget does not wait");
    }

    @Override
    public Cancellable after(long delayMillis, Runnable task) {
      throw new IllegalStateException("an unlimited budget does not wait");
    }
  };
}
