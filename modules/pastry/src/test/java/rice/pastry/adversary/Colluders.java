package rice.pastry.adversary;

import rice.environment.random.RandomSource;

/**
 * What the malicious nodes of a run share: the attack switch, the colluder registry (through the oracle) and the
 * harness random. Implemented by {@link AdversarialRing}; used on the selector thread only.
 */
interface Colluders {

  /** True from the attack start on. */
  boolean isAttacking();

  Oracle oracle();

  RandomSource random();

  Scenario scenario();

  /** Counts a routed message a malicious node dropped. */
  void messageDropped();

  /** Counts a routed message a malicious node misrouted. */
  void messageMisrouted();
}
