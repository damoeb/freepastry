package rice.p2p.aggregation.messaging;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class NonAggregateTest {

  /** Remembers the existing content it was asked to check against. */
  private static final class Recorder implements PastContent {
    PastContent seen;

    @Override
    public PastContent checkInsert(Id id, PastContent existingContent) {
      seen = existingContent;
      return this;
    }

    @Override
    public PastContentHandle getHandle(Past local) {
      return null;
    }

    @Override
    public Id getId() {
      return null;
    }

    @Override
    public boolean isMutable() {
      return false;
    }

    @Override
    public short getType() {
      return 0;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {}
  }

  @Test
  void checkInsertAcceptsExistingContentThatIsStoredUnwrapped() throws Exception {
    Recorder inserted = new Recorder();
    Recorder stored = new Recorder();

    new NonAggregate(inserted).checkInsert(null, stored);

    assertSame(stored, inserted.seen);
  }

  @Test
  void checkInsertUnwrapsExistingContentThatIsWrapped() throws Exception {
    Recorder inserted = new Recorder();
    Recorder stored = new Recorder();

    new NonAggregate(inserted).checkInsert(null, new NonAggregate(stored));

    assertSame(stored, inserted.seen);
  }

  @Test
  void checkInsertAcceptsNoExistingContent() throws Exception {
    Recorder inserted = new Recorder();

    new NonAggregate(inserted).checkInsert(null, null);

    assertNull(inserted.seen);
  }
}
