package rice.pastry.socket.internet;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import rice.environment.Environment;
import rice.pastry.socket.nat.NoOpFirewallPortOpener;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** {@code nat_handler_class} names a class that is loaded by reflection: only a FirewallPortOpener may be loaded, and nothing else may run. */
class NatHandlerClassTest {
  static final AtomicBoolean STATIC_INITIALIZER_RAN = new AtomicBoolean();

  /** a class that is no FirewallPortOpener and does something when it is initialized */
  public static class NotAnOpener {
    static {
      STATIC_INITIALIZER_RAN.set(true);
    }
  }

  private Environment environment;

  @BeforeEach
  void setUp() {
    STATIC_INITIALIZER_RAN.set(false);
    environment = new Environment();
  }

  @AfterEach
  void tearDown() {
    environment.destroy();
  }

  @Test
  void classThatIsNoFirewallPortOpenerIsRefusedAndNotInitialized() {
    assertThrows(IllegalArgumentException.class,
        () -> InternetPastryNodeFactory.loadNatHandler(NotAnOpener.class.getName(), environment, InetAddress.getLoopbackAddress(), LoggerFactory.getLogger(NatHandlerClassTest.class)));

    // one test for both: the initializer runs once per JVM, so a second test could not see it
    assertFalse(STATIC_INITIALIZER_RAN.get(), "the static initializer of a class that is refused must not run");
  }

  @Test
  void firewallPortOpenerIsLoaded() {
    Object actual = InternetPastryNodeFactory.loadNatHandler(NoOpFirewallPortOpener.class.getName(), environment, InetAddress.getLoopbackAddress(), LoggerFactory.getLogger(NatHandlerClassTest.class));

    assertInstanceOf(NoOpFirewallPortOpener.class, actual);
  }
}
