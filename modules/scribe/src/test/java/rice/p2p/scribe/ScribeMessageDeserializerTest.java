package rice.p2p.scribe;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.slf4j.helpers.NOPLogger;
import rice.p2p.util.rawserialization.SimpleInputBuffer;

class ScribeMessageDeserializerTest {
  private static final short UNKNOWN_TYPE = 12345;

  @Test
  void rejectsAnUnknownTypeWithAnIOException() {
    ScribeMessageDeserializer target = new ScribeMessageDeserializer(null, () -> null, "test", NOPLogger.NOP_LOGGER);

    IOException actual = assertThrows(IOException.class,
        () -> target.deserialize(new SimpleInputBuffer(new byte[0]), UNKNOWN_TYPE, 0, null));

    assertTrue(actual.getMessage().contains(String.valueOf(UNKNOWN_TYPE)), actual.getMessage());
  }
}
