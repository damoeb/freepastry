package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class EntityManagerRegistryTest {
  private final EntityManagerContext<String> ctx = new EntityManagerContext<>(null, null, null, null,
      LoggerFactory.getLogger(EntityManagerRegistryTest.class), 10, 10, 1024, List.of(), new TransportListeners<>(),
      () -> null, () -> null, () -> false, id -> null);

  private final EntityManagerRegistry<String> target = new EntityManagerRegistry<>(id -> new EntityManager<>(id, ctx));

  @Test
  void returnsTheSameManagerForAnIdentifier() {
    assertSame(target.get("a", true), target.get("a", true));
  }

  @Test
  void idleManagersAreDroppedWhenTheRegistryHasGrown() {
    for (int i = 0; i < EntityManagerRegistry.MIN_SWEEP_SIZE; i++) {
      target.get("peer" + i, true);
    }

    target.get("one more", true);

    assertEquals(1, target.size());
  }

  @Test
  void aManagerWithASocketIsKept() {
    EntityManager<String> busy = target.get("busy", true);
    busy.sockets.add(new FakeSocket());
    for (int i = 0; i < EntityManagerRegistry.MIN_SWEEP_SIZE; i++) {
      target.get("peer" + i, true);
    }

    target.get("one more", true);

    assertTrue(target.size() < EntityManagerRegistry.MIN_SWEEP_SIZE);
    assertSame(busy, target.get("busy", true));
  }

  @Test
  void nothingIsDroppedOffTheSelectorThread() {
    for (int i = 0; i < EntityManagerRegistry.MIN_SWEEP_SIZE + 10; i++) {
      target.get("peer" + i, false);
    }

    assertEquals(EntityManagerRegistry.MIN_SWEEP_SIZE + 10, target.size());
  }

  @Test
  void forgetsAnIdleManager() {
    target.get("a", true);

    target.forgetIfIdle("a");

    assertEquals(0, target.size());
  }

  @Test
  void keepsABusyManagerWhenAskedToForgetIt() {
    EntityManager<String> busy = target.get("busy", true);
    busy.sockets.add(new FakeSocket());

    target.forgetIfIdle("busy");

    assertEquals(1, target.size());
  }
}
