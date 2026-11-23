package rice.environment.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;
import org.slf4j.event.SubstituteLoggingEvent;
import org.slf4j.helpers.SubstituteLogger;

class LogRateLimiterTest {
  private static final long INTERVAL_MILLIS = 1000;

  private long nowMillis = 0;
  private final ConcurrentLinkedQueue<SubstituteLoggingEvent> events = new ConcurrentLinkedQueue<>();
  /** a logger that records its events, at every level */
  private final SubstituteLogger logger = new SubstituteLogger("test", events, false);
  private final LogRateLimiter testObj = new LogRateLimiter(() -> nowMillis, 3, INTERVAL_MILLIS);

  private List<String> messages() {
    List<String> messages = new ArrayList<>();
    for (SubstituteLoggingEvent event : events) messages.add(event.getMessage());
    return messages;
  }

  @Test
  void logsTheFirstLinesOfAKindPerIntervalThenSumsUpTheRest() {
    for (int i = 0; i < 10; i++) testObj.at(logger, Level.WARN, "a").log("line {}", i);

    assertEquals(List.of("line {}", "line {}", "line {}"), messages());

    nowMillis += INTERVAL_MILLIS;
    testObj.at(logger, Level.WARN, "a").log("later");

    assertEquals(5, events.size());
    assertTrue(messages().get(3).startsWith("Suppressed {}"), messages().get(3));
    assertEquals(7L, events.stream().toList().get(3).getArgumentArray()[0]);
    assertEquals("later", messages().get(4));
  }

  @Test
  void kindsAreLimitedSeparately() {
    for (int i = 0; i < 5; i++) {
      testObj.at(logger, Level.INFO, "a").log("a");
      testObj.at(logger, Level.INFO, "b").log("b");
    }

    assertEquals(6, events.size());
  }

  @Test
  void keysBeyondTheMaximumShareOneWindow() {
    for (int i = 0; i < LogRateLimiter.MAX_KEYS + 100; i++) testObj.at(logger, Level.INFO, "key" + i).log("x");

    assertEquals(LogRateLimiter.MAX_KEYS + 3, events.size());
  }
}
