package rice.pastry.adversary;

import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.NetworkSimulator;

/**
 * The network of the harness: positions from {@link FailureDomains}, and the {@link Behaviour.FastPing} lie applied to
 * the proximity an honest node measures to a colluder. The network delay itself stays true; only what the proximity
 * provider of the measuring node reports is changed, as with a pong forger or a colluder hosted next to the victim.
 */
final class AdversarialNetwork extends NetworkSimulator<DirectNodeHandle, RawMessage> {

  /** Decides the factor for a measurement; 1 means the truth. */
  interface ProximityLie {
    double factor(DirectNodeHandle measurer, DirectNodeHandle measured);
  }

  private final ProximityLie lie;

  AdversarialNetwork(Environment env, FailureDomains domains, ProximityLie lie) {
    super(env, domains);
    this.lie = lie;
  }

  @Override
  public float proximity(DirectNodeHandle measurer, DirectNodeHandle measured) {
    float truth = super.proximity(measurer, measured);
    double factor = lie.factor(measurer, measured);
    if (factor == 1.0) return truth;
    return Math.max(1.0f, (float) Math.floor(truth * factor));
  }
}
