package org.mpisws.p2p.transport.priority;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * The {@link EntityManager} of each identifier we talk to. A manager is created on demand, so a peer that
 * connects from many identifiers would make the map grow forever; therefore the managers that hold no state
 * (see {@link EntityManager#isIdle()}) are dropped whenever the map has doubled since the last sweep.
 * A dropped manager is recreated the next time its identifier is used.
 */
final class EntityManagerRegistry<Identifier> {
  /** Below this many managers there is nothing worth sweeping. */
  static final int MIN_SWEEP_SIZE = 256;

  private final Map<Identifier, EntityManager<Identifier>> managers = new HashMap<>();
  private final Function<Identifier, EntityManager<Identifier>> factory;
  private int nextSweepAt = MIN_SWEEP_SIZE;

  EntityManagerRegistry(Function<Identifier, EntityManager<Identifier>> factory) {
    this.factory = factory;
  }

  /**
   * @param canSweep true on the thread that owns the managers' state (the selector), the only one that can tell
   *        whether a manager is idle
   */
  synchronized EntityManager<Identifier> get(Identifier identifier, boolean canSweep) {
    EntityManager<Identifier> manager = managers.get(identifier);
    if (manager == null) {
      if (canSweep && managers.size() >= nextSweepAt) {
        sweep();
      }
      manager = factory.apply(identifier);
      managers.put(identifier, manager);
    }
    return manager;
  }

  /**
   * Drops the manager of {@code identifier} if it holds no state, for example after the node was declared dead.
   */
  synchronized void forgetIfIdle(Identifier identifier) {
    EntityManager<Identifier> manager = managers.get(identifier);
    if (manager != null && manager.isIdle()) {
      managers.remove(identifier);
    }
  }

  synchronized int size() {
    return managers.size();
  }

  private void sweep() {
    managers.values().removeIf(EntityManager::isIdle);
    nextSweepAt = Math.max(MIN_SWEEP_SIZE, managers.size() * 2);
  }
}
