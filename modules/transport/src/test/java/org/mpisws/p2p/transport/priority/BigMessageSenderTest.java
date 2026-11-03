package org.mpisws.p2p.transport.priority;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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
}
