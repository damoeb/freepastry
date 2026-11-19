package rice.selector;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.Test;
import rice.environment.logging.Slf4jLogManager;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.simple.SimpleTimeSource;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SelectorManagerSelectTest {

  /** A selector whose selects fail with the given exception. */
  static class FailingSelector extends Selector {
    final IOException failure;
    FailingSelector(IOException failure) { this.failure = failure; }
    @Override public boolean isOpen() { return true; }
    @Override public SelectorProvider provider() { return SelectorProvider.provider(); }
    @Override public Set<SelectionKey> keys() { return Collections.emptySet(); }
    @Override public Set<SelectionKey> selectedKeys() { return Collections.emptySet(); }
    @Override public int selectNow() throws IOException { throw failure; }
    @Override public int select(long timeout) throws IOException { throw failure; }
    @Override public int select() throws IOException { throw failure; }
    @Override public Selector wakeup() { return this; }
    @Override public void close() { }
  }

  @Test
  void selectRethrowsAnIOExceptionWithoutMessage() throws IOException {
    SelectorLoop manager = new SelectorLoop("test", new SimpleTimeSource(), new Slf4jLogManager(), new SimpleRandomSource(1, null));
    manager.selector.close();
    IOException failure = new IOException();
    manager.selector = new FailingSelector(failure);

    IOException thrown = assertThrows(IOException.class, () -> manager.select(0));

    assertSame(failure, thrown);
  }
}
