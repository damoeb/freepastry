package rice.testing.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import java.io.IOException;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;

/**
 * Shared pieces of the Jazzer fuzz tests of the wire deserializers. The invariant every target checks: given arbitrary
 * bytes, a deserializer returns a value or throws an {@link IOException}. Any other exception, an out-of-memory error
 * or a huge allocation is a finding.
 */
public final class FuzzSupport {
  /** The message type ids of every family are small numbers, so most generated types stay below this. */
  private static final short SMALL_TYPE_LIMIT = 40;

  private FuzzSupport() {
  }

  /** A message type id, a priority and the rest of the input as the message body. */
  public record Message(short type, int priority, InputBuffer body) {
    public static Message of(FuzzedDataProvider data) {
      short type = data.consumeBoolean() ? data.consumeShort((short) 0, SMALL_TYPE_LIMIT) : data.consumeShort();
      int priority = data.consumeInt();
      return new Message(type, priority, buffer(data));
    }
  }

  /** The rest of the input as an {@link InputBuffer}. */
  public static InputBuffer buffer(FuzzedDataProvider data) {
    return new SimpleInputBuffer(data.consumeRemainingAsBytes());
  }

  /** Runs the deserializer on the input; an {@link IOException} is the accepted way to reject it. */
  public static void deserialize(MessageDeserializer target, FuzzedDataProvider data, NodeHandle sender) {
    Message message = Message.of(data);
    try {
      target.deserialize(message.body(), message.type(), message.priority(), sender);
    } catch (IOException expected) {
      // the accepted way to reject malformed input
    }
  }

  /** Something that reads a value from a buffer. */
  @FunctionalInterface
  public interface Reader {
    void read(InputBuffer buf) throws IOException;
  }

  /** Reads from the rest of the input; an {@link IOException} is the accepted way to reject it. */
  public static void read(FuzzedDataProvider data, Reader reader) {
    try {
      reader.read(buffer(data));
    } catch (IOException expected) {
      // the accepted way to reject malformed input
    }
  }
}
