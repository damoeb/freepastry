package rice.persistence.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.SortedMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.Continuation;
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;
import rice.persistence.MemoryStorage;

/**
 * Regression test: the metadata scans of a MemoryStorage return snapshots, not views of the live index.
 */
public class MemoryStorageSnapshotTest {

  private Environment env;
  private PastryIdFactory factory;
  private MemoryStorage storage;

  @BeforeEach
  public void setUp() {
    env = Environment.directEnvironment();
    factory = new PastryIdFactory(env);
    storage = new MemoryStorage(factory);
  }

  @AfterEach
  public void tearDown() {
    env.destroy();
  }

  private void store(Id id) {
    Futures.forward(storage.store(id, "meta", "value"), new Continuation<Boolean, Exception>() {
      @Override
      public void receiveResult(Boolean result) {
      }

      @Override
      public void receiveException(Exception exception) {
        throw new AssertionError(exception);
      }
    });
  }

  @Test
  public void scanMetadataIsNotAffectedByLaterStores() {
    store(factory.buildId("a"));
    SortedMap<Id, Object> snapshot = storage.scanMetadata();
    assertEquals(1, snapshot.size());

    store(factory.buildId("b"));

    assertEquals(1, snapshot.size());
  }

  @Test
  public void scanMetadataOfARangeIsNotAffectedByLaterStores() {
    store(factory.buildId("a"));
    SortedMap<Id, Object> snapshot = storage.scanMetadata(factory.buildIdRange(factory.buildId(new byte[20]), factory.buildId(new byte[] {
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1})));
    assertEquals(1, snapshot.size());

    store(factory.buildId("b"));

    assertEquals(1, snapshot.size());
  }
}
