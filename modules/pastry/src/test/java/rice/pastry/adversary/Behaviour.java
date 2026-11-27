package rice.pastry.adversary;

import org.jspecify.annotations.Nullable;
import rice.pastry.IdRange;

/**
 * What a malicious node does. Behaviours combine: a scenario gives every malicious node the same set. Each one is
 * injected through a seam of the node factory, see {@link AdversarialDirectPastryNodeFactory}.
 */
public sealed interface Behaviour {

  /** A short name for tables and metric lines. */
  String label();

  /**
   * Accept routed messages and drop them, with {@code probability}, and only those whose key lies in
   * {@code targetRange} when it is set. The upstream hop has already had its hop ack, so the drop is silent. A
   * dropping node that is itself the root does not deliver either.
   */
  record Drop(double probability, @Nullable IdRange targetRange) implements Behaviour {
    public Drop() {
      this(1.0, null);
    }

    @Override
    public String label() {
      return probability >= 1.0 && targetRange == null ? "Drop" : "Drop(" + probability + ")";
    }
  }

  /** Forward every routed message to a colluder instead of the next hop; that colluder claims to be the root. */
  record Misroute(MisrouteTarget target) implements Behaviour {
    public Misroute() {
      this(MisrouteTarget.CLOSEST_COLLUDER);
    }

    @Override
    public String label() {
      return target == MisrouteTarget.CLOSEST_COLLUDER ? "Misroute" : "Misroute(" + target + ")";
    }
  }

  /** Which colluder a misrouting node forwards to. */
  enum MisrouteTarget {
    /** The live colluder whose id is closest to the key. */
    CLOSEST_COLLUDER,
    /** A random live colluder. */
    RANDOM_COLLUDER
  }

  /** As the root of a join, answer the joiner with a leaf set of colluders only. */
  record LieInJoin() implements Behaviour {
    @Override
    public String label() {
      return "LieInJoin";
    }
  }

  /**
   * Answer every route-row request with a row of colluders only, and push such rows unsolicited to an honest node every
   * {@code pushIntervalMillis}.
   */
  record PoisonRows(long pushIntervalMillis) implements Behaviour {
    public PoisonRows() {
      this(DEFAULT_PUSH_INTERVAL_MILLIS);
    }

    @Override
    public String label() {
      return "PoisonRows";
    }
  }

  /** Broadcast leaf sets of colluders to the honest leaf-set neighbours every {@code pushIntervalMillis}. */
  record PoisonLeafSet(long pushIntervalMillis) implements Behaviour {
    public PoisonLeafSet() {
      this(DEFAULT_PUSH_INTERVAL_MILLIS);
    }

    @Override
    public String label() {
      return "PoisonLeafSet";
    }
  }

  /**
   * Appear closer in RTT than the topology says: the proximity an honest node measures to a colluder is multiplied
   * by {@code factor}. Models colluders hosted next to every honest node, or an on-path pong forger. It is a property
   * of the topology, so it holds from the start of the run, not from the attack start.
   */
  record FastPing(double factor) implements Behaviour {
    public static final double DEFAULT_FACTOR = 0.1;

    public FastPing() {
      this(DEFAULT_FACTOR);
    }

    @Override
    public String label() {
      return "FastPing";
    }
  }

  /** Behave honestly for maintenance (joins): the dropping and misrouting apply to application traffic only. */
  record Selective() implements Behaviour {
    @Override
    public String label() {
      return "Selective";
    }
  }

  /** Leave and rejoin with a fresh id {@code rotationsPerHour} times per hour on average, to re-roll table slots. */
  record ChurnRotate(double rotationsPerHour) implements Behaviour {
    @Override
    public String label() {
      return "ChurnRotate";
    }
  }

  long DEFAULT_PUSH_INTERVAL_MILLIS = 60_000;
}
