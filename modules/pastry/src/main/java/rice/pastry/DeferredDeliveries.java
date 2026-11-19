package rice.pastry;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Certified mode: holds the messages of peers that are not authenticated yet, in order, until the check
 * ends. A peer is whatever the caller checks, a node handle in {@code PastryNode}. A peer whose id is verified gets its messages delivered; if the check fails or takes longer than
 * {@link #VERIFY_TIMEOUT_MILLIS}, they are dropped. Memory is bounded: {@link #MAX_PER_PEER} messages per
 * peer and {@link #MAX_TOTAL} overall, so a flood of made-up ids cannot exhaust it; a message over a bound is
 * dropped. Selector thread only, like the code that calls it.
 *
 * @param <P> the peer
 */
public class DeferredDeliveries<P> {

  /** messages kept per unverified peer */
  public static final int MAX_PER_PEER = 8;

  /** messages kept for all unverified peers together */
  public static final int MAX_TOTAL = 64;

  /** how long a peer's messages wait for its id to be verified */
  public static final long VERIFY_TIMEOUT_MILLIS = 10_000;

  private static final Logger logger = LoggerFactory.getLogger(DeferredDeliveries.class);

  private final BiConsumer<Runnable, Long> scheduler;
  private final Map<P, Queue<Runnable>> pending = new HashMap<>();
  private int total;

  /**
   * @param scheduler runs the given task once after the given delay in milliseconds, on the selector thread
   */
  public DeferredDeliveries(BiConsumer<Runnable, Long> scheduler) {
    this.scheduler = scheduler;
  }

  /**
   * Keeps a delivery until {@link #resolve} is called for the peer.
   *
   * @return false if a bound is reached and the delivery was dropped
   */
  @SuppressWarnings("ReferenceEquality") // the queue of this very wait, not an equal one
  public boolean defer(P peer, Runnable delivery) {
    Queue<Runnable> queue = pending.get(peer);
    int waiting = queue == null ? 0 : queue.size();
    if (waiting >= MAX_PER_PEER || total >= MAX_TOTAL) {
      logger.debug("Dropping a message from unverified {}: {} waiting for it, {} in all", peer, waiting, total);
      return false;
    }
    if (queue == null) {
      Queue<Runnable> started = new ArrayDeque<>();
      queue = started;
      pending.put(peer, started);
      scheduler.accept(() -> {
        if (pending.get(peer) == started) {
          logger.debug("Dropping {} messages from {}: not verified within {} ms", started.size(), peer, VERIFY_TIMEOUT_MILLIS);
          resolve(peer, false);
        }
      }, VERIFY_TIMEOUT_MILLIS);
    }
    queue.add(delivery);
    total++;
    return true;
  }

  /**
   * Ends the wait for a peer: delivers its messages in order if it is authenticated, drops them otherwise.
   */
  public void resolve(P peer, boolean authenticated) {
    Queue<Runnable> queue = pending.remove(peer);
    if (queue == null) {
      return;
    }
    total -= queue.size();
    if (!authenticated) {
      logger.debug("Dropping {} messages from {}: its id could not be verified", queue.size(), peer);
      return;
    }
    for (Runnable delivery : queue) {
      delivery.run();
    }
  }

  /** @return true while messages of this peer wait for its check to end */
  public boolean isWaiting(P peer) {
    return pending.containsKey(peer);
  }

  /** @return the number of messages waiting in all */
  public int size() {
    return total;
  }
}
