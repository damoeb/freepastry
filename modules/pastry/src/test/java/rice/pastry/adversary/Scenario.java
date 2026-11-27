package rice.pastry.adversary;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;

/**
 * One run of the harness: ring size, adversary, churn, failure domains, workload and timeline. Build one with
 * {@link #builder()}.
 *
 * <p>Timeline (simulated time): the ring is built with {@code nodeCount} joins, one every {@code joinIntervalMillis},
 * every node joining through a random ready node; the ring then stabilises for {@code stabiliseMillis}; the
 * <em>pre-attack window</em> ({@code preAttackWindowMillis}) measures the honest baseline; at the <em>attack start</em>
 * the behaviours become active, churn starts and the optional domain outage happens; the <em>attack window</em>
 * ({@code attackWindowMillis}) carries the lookups, the late honest joins and the churn; the tables are measured at
 * its end. Malicious nodes are part of the ring from the start and behave honestly before the attack start, except
 * for {@link Behaviour.FastPing}, which is a property of the topology.
 */
public record Scenario(
    long seed,
    int nodeCount,
    double maliciousFraction,
    Placement placement,
    List<Behaviour> behaviours,
    Optional<Churn> churn,
    int domainCount,
    double domainRadiusFraction,
    boolean hasDomainOutageAtAttackStart,
    double lookupsPerSecond,
    int lateJoinCount,
    long joinIntervalMillis,
    long stabiliseMillis,
    long preAttackWindowMillis,
    long attackWindowMillis,
    long lookupDeadlineMillis,
    long recoveryBucketMillis,
    boolean isByteEstimateEnabled) {

  public Scenario {
    behaviours = List.copyOf(behaviours);
    if (nodeCount < 2) throw new IllegalArgumentException("nodeCount must be at least 2");
    if (!(lookupsPerSecond > 0)) throw new IllegalArgumentException("lookupsPerSecond must be positive");
    if (maliciousFraction < 0 || maliciousFraction >= 1) {
      throw new IllegalArgumentException("maliciousFraction must be in [0, 1): " + maliciousFraction);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  public int maliciousCount() {
    return (int) Math.round(nodeCount * maliciousFraction);
  }

  public boolean has(Class<? extends Behaviour> type) {
    return behaviour(type) != null;
  }

  public <T extends Behaviour> @Nullable T behaviour(Class<T> type) {
    for (Behaviour behaviour : behaviours) {
      if (type.isInstance(behaviour)) return type.cast(behaviour);
    }
    return null;
  }

  /** The behaviours as {@code A+B}, or {@code none}. */
  public String behaviourLabel() {
    if (behaviours.isEmpty()) return "none";
    return behaviours.stream().map(Behaviour::label).collect(Collectors.joining("+"));
  }

  public String churnLabel() {
    return churn.map(Churn::label).orElse("none");
  }

  /** Identifies the cell of the matrix, without the seed. */
  public String cellLabel() {
    return "N=" + nodeCount + " f=" + maliciousFraction + " " + behaviourLabel() + " " + placement + " churn="
        + churnLabel() + (hasDomainOutageAtAttackStart ? " outage" : "");
  }

  /** Mutable builder with the defaults of the baseline matrix. */
  public static final class Builder {
    private long seed = 1;
    private int nodeCount = 200;
    private double maliciousFraction;
    private Placement placement = new Placement.Uniform();
    private List<Behaviour> behaviours = new ArrayList<>();
    private Optional<Churn> churn = Optional.empty();
    private int domainCount = 16;
    private double domainRadiusFraction = 0.05;
    private boolean hasDomainOutageAtAttackStart;
    private double lookupsPerSecond = 2.0;
    private int lateJoinCount = -1;
    private long joinIntervalMillis = 500;
    private long stabiliseMillis = 120_000;
    private long preAttackWindowMillis = 120_000;
    private long attackWindowMillis = 600_000;
    private long lookupDeadlineMillis = 30_000;
    private long recoveryBucketMillis = 30_000;
    private boolean isByteEstimateEnabled;

    Builder() {}

    Builder(Scenario scenario) {
      seed = scenario.seed;
      nodeCount = scenario.nodeCount;
      maliciousFraction = scenario.maliciousFraction;
      placement = scenario.placement;
      behaviours = new ArrayList<>(scenario.behaviours);
      churn = scenario.churn;
      domainCount = scenario.domainCount;
      domainRadiusFraction = scenario.domainRadiusFraction;
      hasDomainOutageAtAttackStart = scenario.hasDomainOutageAtAttackStart;
      lookupsPerSecond = scenario.lookupsPerSecond;
      lateJoinCount = scenario.lateJoinCount;
      joinIntervalMillis = scenario.joinIntervalMillis;
      stabiliseMillis = scenario.stabiliseMillis;
      preAttackWindowMillis = scenario.preAttackWindowMillis;
      attackWindowMillis = scenario.attackWindowMillis;
      lookupDeadlineMillis = scenario.lookupDeadlineMillis;
      recoveryBucketMillis = scenario.recoveryBucketMillis;
      isByteEstimateEnabled = scenario.isByteEstimateEnabled;
    }

    public Builder seed(long value) {
      seed = value;
      return this;
    }

    public Builder nodes(int value) {
      nodeCount = value;
      return this;
    }

    public Builder maliciousFraction(double value) {
      maliciousFraction = value;
      return this;
    }

    public Builder placement(Placement value) {
      placement = value;
      return this;
    }

    public Builder behaviours(Behaviour... values) {
      behaviours = new ArrayList<>(List.of(values));
      return this;
    }

    public Builder behaviours(List<Behaviour> values) {
      behaviours = new ArrayList<>(values);
      return this;
    }

    public Builder churn(@Nullable Churn value) {
      churn = Optional.ofNullable(value);
      return this;
    }

    public Builder domains(int value) {
      domainCount = value;
      return this;
    }

    public Builder domainRadiusFraction(double value) {
      domainRadiusFraction = value;
      return this;
    }

    public Builder domainOutageAtAttackStart(boolean value) {
      hasDomainOutageAtAttackStart = value;
      return this;
    }

    /** Lookups started per simulated second, in both measurement windows. */
    public Builder lookupsPerSecond(double value) {
      lookupsPerSecond = value;
      return this;
    }

    /** Honest nodes that join during the attack window; the default is 5 % of the ring, at least 5. */
    public Builder lateJoins(int value) {
      lateJoinCount = value;
      return this;
    }

    public Builder joinIntervalMillis(long value) {
      joinIntervalMillis = value;
      return this;
    }

    public Builder stabiliseMillis(long value) {
      stabiliseMillis = value;
      return this;
    }

    public Builder preAttackWindowMillis(long value) {
      preAttackWindowMillis = value;
      return this;
    }

    public Builder attackWindowMillis(long value) {
      attackWindowMillis = value;
      return this;
    }

    public Builder lookupDeadlineMillis(long value) {
      lookupDeadlineMillis = value;
      return this;
    }

    public Builder recoveryBucketMillis(long value) {
      recoveryBucketMillis = value;
      return this;
    }

    public Builder byteEstimate(boolean value) {
      isByteEstimateEnabled = value;
      return this;
    }

    public Scenario build() {
      int lateJoins = lateJoinCount >= 0 ? lateJoinCount : Math.max(5, nodeCount / 20);
      return new Scenario(seed, nodeCount, maliciousFraction, placement, behaviours, churn, domainCount,
          domainRadiusFraction, hasDomainOutageAtAttackStart, lookupsPerSecond, lateJoins, joinIntervalMillis,
          stabiliseMillis, preAttackWindowMillis, attackWindowMillis, lookupDeadlineMillis, recoveryBucketMillis,
          isByteEstimateEnabled);
    }
  }
}
