package rice.pastry.standard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import rice.pastry.standard.ConsistentJoinProtocol.FailedTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * FailedTime.compareTo() used to cast the difference of two times to int, so two failures more than about 24 days
 * apart compared the wrong way round.
 */
class FailedTimeTest {

  private static final long THIRTY_DAYS_MILLIS = 30L * 24 * 60 * 60 * 1000;

  @Test
  void compareToSortsTheMostRecentFailureFirst() {
    FailedTime older = new FailedTime(null, 1_000L);
    FailedTime newer = new FailedTime(null, 2_000L);
    List<FailedTime> times = new ArrayList<FailedTime>(List.of(older, newer));

    Collections.sort(times);

    assertSame(newer, times.get(0));
  }

  @Test
  void compareToKeepsTheOrderForFailuresMoreThanAnIntRangeApart() {
    FailedTime older = new FailedTime(null, 0L);
    FailedTime newer = new FailedTime(null, THIRTY_DAYS_MILLIS);

    assertEquals(1, Integer.signum(older.compareTo(newer)));
    assertEquals(-1, Integer.signum(newer.compareTo(older)));
  }
}
