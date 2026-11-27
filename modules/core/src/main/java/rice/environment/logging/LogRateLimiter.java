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
package rice.environment.logging;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;
import org.slf4j.spi.NOPLoggingEventBuilder;
import rice.environment.params.Parameters;

/**
 * Limits the log lines of one kind (a key, typically one call site) to a number per interval, so that a peer that
 * sends malformed input as fast as it can neither fills the disk nor stalls the selector thread in a synchronous
 * appender. The lines of a kind beyond the limit are counted, and the first line of that kind in a later interval is
 * preceded by a summary line with that count.
 * <p>
 * The keys must be a fixed set (call sites, never a remote address): at most {@link #MAX_KEYS} keys are tracked, any
 * further key shares one window. The limiter is thread safe; it holds no lock of its own.
 */
public final class LogRateLimiter {
  /** the parameter for the lines of one kind that are logged per interval */
  public static final String MAX_LINES_PARAM = "logging_rate_limit_max_lines";
  /** the parameter for the length of the interval, in milliseconds */
  public static final String INTERVAL_PARAM = "logging_rate_limit_interval_millis";
  static final int DEFAULT_MAX_LINES = 10;
  static final long DEFAULT_INTERVAL_MILLIS = 60_000;
  /** the most keys with a window of their own; any further key shares {@link #OTHER_KEY} */
  static final int MAX_KEYS = 256;
  static final String OTHER_KEY = "(other)";

  /**
   * The lines of one kind in the current interval.
   *
   * @param startMillis when the interval started
   * @param loggedLines the lines logged in it
   * @param suppressedLines the lines not logged in it
   * @param isLastLineLogged whether the line that made this window is logged
   * @param suppressedBefore lines not logged in the interval before, to report before the line that made this window
   */
  private record Window(long startMillis, int loggedLines, long suppressedLines, boolean isLastLineLogged,
      long suppressedBefore) {
  }

  private final LongSupplier clockMillis;
  private final int maxLines;
  private final long intervalMillis;
  private final Map<String, Window> windows = new ConcurrentHashMap<String, Window>();

  /**
   * @param clockMillis the clock, in milliseconds
   * @param maxLines the lines of one kind that are logged per interval
   * @param intervalMillis the length of the interval
   */
  public LogRateLimiter(LongSupplier clockMillis, int maxLines, long intervalMillis) {
    if (maxLines < 0) throw new IllegalArgumentException("maxLines " + maxLines + " < 0");
    if (intervalMillis <= 0) throw new IllegalArgumentException("intervalMillis " + intervalMillis + " <= 0");
    this.clockMillis = clockMillis;
    this.maxLines = maxLines;
    this.intervalMillis = intervalMillis;
  }

  /**
   * @param parameters may hold {@link #MAX_LINES_PARAM} and {@link #INTERVAL_PARAM}; the defaults apply otherwise
   * @param clockMillis the clock, in milliseconds
   * @return a limiter configured from the parameters
   */
  public static LogRateLimiter fromParameters(Parameters parameters, LongSupplier clockMillis) {
    int maxLines = parameters.contains(MAX_LINES_PARAM) ? parameters.getInt(MAX_LINES_PARAM) : DEFAULT_MAX_LINES;
    long intervalMillis = parameters.contains(INTERVAL_PARAM)
        ? parameters.getLong(INTERVAL_PARAM) : DEFAULT_INTERVAL_MILLIS;
    return new LogRateLimiter(clockMillis, maxLines, intervalMillis);
  }

  /**
   * @return a limiter with the default limits on the system clock, for code without an Environment at hand
   */
  public static LogRateLimiter withDefaults() {
    return new LogRateLimiter(System::currentTimeMillis, DEFAULT_MAX_LINES, DEFAULT_INTERVAL_MILLIS);
  }

  /**
   * Starts a log line of this kind if the limit allows it. Use it like {@link Logger#atLevel(Level)}:
   * {@code limiter.at(logger, Level.WARN, "wire.datagram").setCause(e).log("Dropping {}", x)}.
   *
   * @param logger where to log
   * @param level the level of the line
   * @param key the kind of line, a constant
   * @return a builder that logs, or one that does nothing if the line is suppressed or the level is disabled
   */
  public LoggingEventBuilder at(Logger logger, Level level, String key) {
    if (!logger.isEnabledForLevel(level)) return NOPLoggingEventBuilder.singleton();
    String windowKey = windows.size() >= MAX_KEYS && !windows.containsKey(key) ? OTHER_KEY : key;
    long nowMillis = clockMillis.getAsLong();
    Window window = windows.compute(windowKey, (k, old) -> next(old, nowMillis));
    if (!window.isLastLineLogged()) return NOPLoggingEventBuilder.singleton();
    if (window.suppressedBefore() > 0) {
      logger.atLevel(level).log("Suppressed {} log lines of kind {} in the {} ms before the next one", window.suppressedBefore(),
          windowKey, intervalMillis);
    }
    return logger.atLevel(level);
  }

  private Window next(@Nullable Window old, long nowMillis) {
    if (old == null || nowMillis - old.startMillis() >= intervalMillis || nowMillis < old.startMillis()) {
      long suppressedBefore = old == null ? 0 : old.suppressedLines();
      if (maxLines == 0) return new Window(nowMillis, 0, 1, false, 0);
      return new Window(nowMillis, 1, 0, true, suppressedBefore);
    }
    if (old.loggedLines() < maxLines) {
      return new Window(old.startMillis(), old.loggedLines() + 1, old.suppressedLines(), true, 0);
    }
    return new Window(old.startMillis(), old.loggedLines(), old.suppressedLines() + 1, false, 0);
  }
}
