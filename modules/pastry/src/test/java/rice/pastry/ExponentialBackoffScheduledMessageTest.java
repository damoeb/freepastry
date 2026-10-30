package rice.pastry;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import rice.selector.Timer;
import rice.selector.TimerTask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ExponentialBackoffScheduledMessage kept its own cancelled flag next to the one it inherits from TimerTask, and its
 * cancel() returned the old value of that flag: false for the cancel that stopped it, true for every later one.
 */
class ExponentialBackoffScheduledMessageTest {

  private static final long INITIAL_DELAY = 10L;
  private static final long INITIAL_PERIOD = 100L;
  private static final double EXP_BASE = 2.0;
  private static final long MAX_PERIOD = -1L;

  @Test
  void cancelReturnsTrueTheFirstTime() {
    ExponentialBackoffScheduledMessage message = newMessage(new RecordingTimer());

    assertTrue(message.cancel());
  }

  @Test
  void cancelReturnsFalseOnceCancelled() {
    ExponentialBackoffScheduledMessage message = newMessage(new RecordingTimer());
    message.cancel();

    assertFalse(message.cancel());
  }

  @Test
  void cancelStopsTheScheduledTask() {
    RecordingTimer timer = new RecordingTimer();
    ExponentialBackoffScheduledMessage message = newMessage(timer);

    message.cancel();

    assertFalse(timer.scheduled.get(0).cancel());
  }

  @Test
  void runAfterCancelSchedulesNothing() {
    RecordingTimer timer = new RecordingTimer();
    ExponentialBackoffScheduledMessage message = newMessage(timer);
    message.cancel();

    message.run();

    assertEquals(1, timer.scheduled.size());
  }

  private static ExponentialBackoffScheduledMessage newMessage(Timer timer) {
    return new ExponentialBackoffScheduledMessage(null, null, timer, INITIAL_DELAY, INITIAL_PERIOD, EXP_BASE,
        MAX_PERIOD);
  }

  private static final class RecordingTimer implements Timer {
    final List<TimerTask> scheduled = new ArrayList<TimerTask>();

    @Override
    public TimerTask scheduleAtFixedRate(TimerTask task, long delay, long period) {
      return schedule(task);
    }

    @Override
    public TimerTask schedule(TimerTask task, long delay) {
      return schedule(task);
    }

    @Override
    public TimerTask schedule(TimerTask task, long delay, long period) {
      return schedule(task);
    }

    @Override
    public TimerTask schedule(TimerTask task) {
      scheduled.add(task);
      return task;
    }
  }
}
