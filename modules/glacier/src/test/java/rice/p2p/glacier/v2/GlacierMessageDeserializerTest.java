package rice.p2p.glacier.v2;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.glacier.v2.messaging.GlacierMessageType;
import rice.p2p.util.rawserialization.SimpleInputBuffer;

class GlacierMessageDeserializerTest {
  private static final short UNKNOWN_TYPE = 12345;

  private final MessageDeserializer target = GlacierMessageDispatcher.deserializer(null);

  @Test
  void rejectsAnUnknownTypeWithAnIOException() {
    assertThrows(IOException.class,
        () -> target.deserialize(new SimpleInputBuffer(new byte[0]), UNKNOWN_TYPE, 0, null));
  }

  @Test
  void rejectsTheLocalTimeoutTypeWithAnIOException() {
    assertThrows(IOException.class,
        () -> target.deserialize(new SimpleInputBuffer(new byte[0]), GlacierMessageType.TIMEOUT.id(), 0, null));
  }
}
