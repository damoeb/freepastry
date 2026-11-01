package rice.pastry.transport;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

class TLDeserializerTest {
  private static final int ADDRESS = 7;

  private Environment env;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  @Test
  void aDeserializerReturningNullRejectsTheMessage() throws IOException {
    TLDeserializer target = new TLDeserializer(null, env);
    target.setDeserializer(ADDRESS, (buf, type, priority, sender) -> null);
    SimpleOutputBuffer out = new SimpleOutputBuffer();
    out.writeInt(ADDRESS);
    out.writeByte((byte) 0);
    out.writeShort((short) 99);

    assertThrows(IOException.class, () -> target.deserialize(new SimpleInputBuffer(out.getBytes()), null));
  }
}
