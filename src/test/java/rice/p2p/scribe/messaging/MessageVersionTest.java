package rice.p2p.scribe.messaging;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Version 0 of these messages is not the current wire version, so reading it must fail loudly
 * instead of returning null and silently dropping the message.
 */
class MessageVersionTest {

  private static InputBuffer version0() {
    return new SimpleInputBuffer(new byte[] { 0 });
  }

  @Test
  void subscribeAckRejectsVersion0() {
    assertThrows(IOException.class, () -> SubscribeAckMessage.build(version0(), null));
  }

  @Test
  void subscribeRejectsVersion0() {
    assertThrows(IOException.class, () -> SubscribeMessage.buildSM(version0(), null, null));
  }

  @Test
  void unsubscribeRejectsVersion0() {
    assertThrows(IOException.class, () -> UnsubscribeMessage.build(version0(), null));
  }

  @Test
  void subscribeFailedRejectsVersion0() {
    assertThrows(IOException.class, () -> SubscribeFailedMessage.build(version0(), null));
  }
}
