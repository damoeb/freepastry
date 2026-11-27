package rice.p2p.past;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.past.messaging.FetchHandleMessage;
import rice.p2p.past.messaging.InsertMessage;
import rice.p2p.past.messaging.LookupMessage;
import rice.p2p.past.messaging.PastMessage;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.testing.SocketRing;
import rice.persistence.CachedStorageImpl;
import rice.persistence.MemoryStorage;
import rice.persistence.NoOpCache;

/**
 * A malicious certified insider that answers requests of another node: a response counts only for the request it
 * answers, from a node that may answer it, and only if it is of the type of the request. Request ids are random, so
 * a node that did not see a request cannot guess its id.
 *
 * <p>The ring is 00, 80, A0 with one extra replica: the replicas of id 30 are 00 and 80, and A0 is the insider.
 */
class PastResponseBindingTest {
  private static final long SECONDS = 10;
  private static final int MESSAGE_TIMEOUT_MILLIS = 1000;
  private static final int GUESSES = 64;

  private SocketRing ring;
  private PastryIdFactory idFactory;

  @BeforeEach
  void setUp() {
    ring = new SocketRing();
    ring.env.getParameters().setInt("p2p_past_messageTimeout", MESSAGE_TIMEOUT_MILLIS);
    idFactory = new PastryIdFactory(ring.env);
  }

  @AfterEach
  void tearDown() {
    ring.close();
  }

  /** A Past whose behaviour the test switches: it records its requests and drops or bends what it receives. */
  static final class RoguePast extends PastImpl {
    final List<PastMessage> sentRequests = new CopyOnWriteArrayList<PastMessage>();
    volatile boolean isDroppingLookups;
    volatile boolean isDroppingInserts;
    volatile boolean isAnsweringFetchHandleWithContent;

    RoguePast(Node node, PastryIdFactory idFactory) {
      super(node, new CachedStorageImpl(idFactory, new MemoryStorage(idFactory), new NoOpCache(idFactory)), 1, "binding");
      setContentDeserializer((buf, endpoint, type) -> new GCPastInsertTest.Content(buf, endpoint));
    }

    @Override
    protected <T> CompletableFuture<T> sendRequest(Id id, PastMessage message, NodeHandle hint) {
      sentRequests.add(message);
      return super.sendRequest(id, message, hint);
    }

    @Override
    protected void deliverInsert(InsertMessage insertMessage) {
      if (!isDroppingInserts) {
        super.deliverInsert(insertMessage);
      }
    }

    @Override
    public void deliver(Id id, Message message) {
      if (isDroppingLookups && message instanceof LookupMessage lookup && !lookup.isResponse()) {
        return;
      }
      if (isAnsweringFetchHandleWithContent && message instanceof FetchHandleMessage request && !request.isResponse()) {
        // answers with a response of another type under the id of the request
        LookupMessage wrongType = new LookupMessage(request.getUID(), request.getId(), request.getSource(), request.getId());
        wrongType.receiveResult(new GCPastInsertTest.Content(request.getId(), false));
        getEndpoint().route(null, wrongType, request.getSource());
        return;
      }
      super.deliver(id, message);
    }
  }

  private List<RoguePast> boot() throws Exception {
    return ring.boot(3, node -> new RoguePast(node, idFactory));
  }

  /** Starts the operation on the selector thread and returns its future. */
  private <T> CompletableFuture<T> onSelector(Supplier<CompletableFuture<T>> operation) throws Exception {
    CompletableFuture<CompletableFuture<T>> started = new CompletableFuture<CompletableFuture<T>>();
    ring.env.getSelectorManager().invoke(() -> started.complete(operation.get()));
    return started.get(SECONDS, TimeUnit.SECONDS);
  }

  private void runOnSelector(Environment env, Runnable task) throws Exception {
    CompletableFuture<Void> done = new CompletableFuture<Void>();
    env.getSelectorManager().invoke(() -> {
      task.run();
      done.complete(null);
    });
    done.get(SECONDS, TimeUnit.SECONDS);
  }

  @Test
  @DisplayName("an insider that guesses the id of a lookup request cannot answer it with forged content")
  void guessedLookupResponseIsIgnored() throws Exception {
    List<RoguePast> pasts = boot();
    RoguePast victim = pasts.get(0);
    RoguePast insider = pasts.get(2);
    for (RoguePast past : pasts) {
      past.isDroppingLookups = true;
    }
    Id id = rice.pastry.Id.build("30");
    GCPastInsertTest.Content forged = new GCPastInsertTest.Content(id, false);

    CompletableFuture<PastContent> found = onSelector(() -> victim.lookup(id, false));
    ring.await("the lookup was sent", SECONDS, () -> !victim.sentRequests.isEmpty());
    runOnSelector(ring.env, () -> {
      // the ids a counter hands out first
      for (int guess = 0; guess < GUESSES; guess++) {
        LookupMessage answer = new LookupMessage(Integer.MIN_VALUE + guess, id, victim.getLocalNodeHandle(), id);
        answer.receiveResult(forged);
        insider.getEndpoint().route(null, answer, victim.getLocalNodeHandle());
      }
    });

    PastContent result = null;
    try {
      result = found.get(SECONDS, TimeUnit.SECONDS);
    } catch (ExecutionException expected) {
      // failing the lookup is as good as finding nothing
    }
    assertNotEquals(forged.getId(), result == null ? null : result.getId(), "the lookup returned the insider's forged content");
  }

  @Test
  @DisplayName("an insider that learned the ids of inserts sent to other nodes cannot acknowledge them")
  void forgedInsertAcknowledgementsAreIgnored() throws Exception {
    List<RoguePast> pasts = boot();
    RoguePast victim = pasts.get(0);
    RoguePast insider = pasts.get(2);
    for (RoguePast past : pasts) {
      past.isDroppingInserts = true;
    }
    GCPastInsertTest.Content content = new GCPastInsertTest.Content(rice.pastry.Id.build("30"), false);

    CompletableFuture<Boolean[]> inserted = onSelector(() -> victim.insert(content));
    ring.await("the inserts were sent", SECONDS, () -> victim.sentRequests.stream().filter(InsertMessage.class::isInstance).count() == 2);
    runOnSelector(ring.env, () -> {
      for (PastMessage request : victim.sentRequests) {
        if (request instanceof InsertMessage insert) {
          InsertMessage acknowledgement = new InsertMessage(insert.getUID(), content, victim.getLocalNodeHandle(), insert.getDestination());
          acknowledgement.receiveResult(true);
          insider.getEndpoint().route(null, acknowledgement, victim.getLocalNodeHandle());
        }
      }
    });

    assertThrows(ExecutionException.class, () -> inserted.get(SECONDS, TimeUnit.SECONDS),
        "the insert succeeded on the insider's acknowledgements, although no replica stored it");
  }

  @Test
  @DisplayName("a response of another type than the request does not complete it")
  void responseOfAnotherTypeIsIgnored() throws Exception {
    List<RoguePast> pasts = boot();
    RoguePast victim = pasts.get(0);
    RoguePast insider = pasts.get(2);
    insider.isAnsweringFetchHandleWithContent = true;
    Id id = rice.pastry.Id.build("30");

    CompletableFuture<?> handle = onSelector(() -> victim.lookupHandle(id, insider.getLocalNodeHandle()));

    assertThrows(ExecutionException.class, () -> handle.get(SECONDS, TimeUnit.SECONDS),
        "a lookup response completed a fetch-handle request");
  }
}
