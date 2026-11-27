package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.slf4j.LoggerFactory;

class BigMessageSenderTest {

  @Test
  @DisplayName("a failed sender removes itself from the pending map by destination")
  void receiveExceptionRemovesTheSenderByItsDestination() {
    Map<String, BigMessageSender<String>> pending = new HashMap<>();
    BigMessageSender<String> testObj =
        new BigMessageSender<>("dest", null, LoggerFactory.getLogger(BigMessageSenderTest.class), pending);
    pending.put("dest", testObj);

    testObj.receiveException(new IOException("boom"));

    assertTrue(pending.isEmpty());
  }

  @Test
  @DisplayName("a failed sender leaves a newer sender for the same destination alone")
  void receiveExceptionKeepsAReplacementSender() {
    Map<String, BigMessageSender<String>> pending = new HashMap<>();
    BigMessageSender<String> old =
        new BigMessageSender<>("dest", null, LoggerFactory.getLogger(BigMessageSenderTest.class), pending);
    BigMessageSender<String> replacement =
        new BigMessageSender<>("dest", null, LoggerFactory.getLogger(BigMessageSenderTest.class), pending);
    pending.put("dest", replacement);

    old.receiveException(new IOException("boom"));

    assertSame(replacement, pending.get("dest"));
  }

  @Test
  @DisplayName("a queued big message carries its destination, message and options")
  void handleCarriesDestinationMessageAndOptions() {
    BigMessageSender<String> testObj =
        new BigMessageSender<>("dest", null, LoggerFactory.getLogger(BigMessageSenderTest.class), new HashMap<>());
    ByteBuffer message = ByteBuffer.allocate(3);
    Map<String, Object> options = Map.of("k", "v");

    MessageRequestHandle<String, ByteBuffer> actual = testObj.addMessage(message, null, options);

    assertEquals("dest", actual.getIdentifier());
    assertSame(message, actual.getMessage());
    assertSame(options, actual.getOptions());
  }

  @Test
  @DisplayName("a big message not yet started is taken out of the queue on cancel")
  void cancelBeforeStartRemovesTheMessage() {
    BigMessageSender<String> testObj =
        new BigMessageSender<>("dest", null, LoggerFactory.getLogger(BigMessageSenderTest.class), new HashMap<>());
    MessageRequestHandle<String, ByteBuffer> handle = testObj.addMessage(ByteBuffer.allocate(3), null, null);

    assertTrue(handle.cancel());
    assertTrue(testObj.msgs.isEmpty());
  }

  @Test
  @DisplayName("a big message that has started writing refuses to cancel")
  void cancelAfterStartIsRefused() {
    BigMessageSender<String> testObj =
        new BigMessageSender<>("dest", null, LoggerFactory.getLogger(BigMessageSenderTest.class), new HashMap<>());
    testObj.addMessage(ByteBuffer.allocate(3), null, null);
    BigMessageSender<String>.PendingMessage started = testObj.msgs.getFirst();
    started.started = true;

    assertFalse(started.cancel());
    assertEquals(1, testObj.msgs.size());
  }
}
