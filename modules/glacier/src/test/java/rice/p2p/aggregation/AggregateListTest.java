package rice.p2p.aggregation;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;

import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rice.environment.Environment;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateListTest {

  private static final int ATTEMPTS = 200;
  private static final int TOLERATED_NEW_DESCRIPTORS = 10;

  @TempDir
  Path dir;

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
  void recoverLogClosesTheLogWhenReplayFails() throws IOException {
    Path config = dir.resolve("aggregates");
    Files.writeString(Path.of(config + ".log"), "this is not a log entry\n");
    PastryIdFactory idFactory = new PastryIdFactory(env);
    UnixOperatingSystemMXBean operatingSystem = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    long before = operatingSystem.getOpenFileDescriptorCount();

    for (int i = 0; i < ATTEMPTS; i++) {
      assertThrows(IllegalStateException.class, () -> new AggregateList(config.toString(), "test", idFactory, true, "test", env));
    }

    long leaked = operatingSystem.getOpenFileDescriptorCount() - before;
    assertTrue(leaked < TOLERATED_NEW_DESCRIPTORS, "file descriptors left open by failed log recoveries: " + leaked);
  }
}
