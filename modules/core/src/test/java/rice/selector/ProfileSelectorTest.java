/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is"
basis, without any representations or warranties of any kind, express or implied
including, but not limited to, representations or warranties of
non-infringement, merchantability or fitness for a particular purpose. In no
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use, data, or
profits; or business interruption) however caused and on any theory of
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if
advised of the possibility of such damage.

*******************************************************************************/
package rice.selector;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.channels.SelectionKey;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rice.environment.logging.LogManager;
import rice.environment.logging.Slf4jLogManager;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.simple.SimpleTimeSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ProfileSelector} without starting its selector thread; the selector loop methods are called directly.
 */
public class ProfileSelectorTest {

  LogManager logManager;
  ProfileSelector selector;
  Pipe pipe;

  @BeforeEach
  public void setUp() throws IOException {
    logManager = new Slf4jLogManager();
    pipe = Pipe.open();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (selector != null) selector.destroy(); // never started: closes the selector
    pipe.source().close();
    pipe.sink().close();
  }

  @Test
  public void keepsTheGivenRandomSource() {
    RandomSource rand = new SimpleRandomSource(1, logManager);
    selector = new ProfileSelector("test", new SimpleTimeSource(), logManager, rand);
    assertSame(rand, selector.getRandom());
  }

  @Test
  public void modifiesEachKeyOnlyOnce() throws IOException {
    selector = new ProfileSelector("test", new SimpleTimeSource(), logManager, null, Integer.MAX_VALUE);
    final int[] modified = new int[1];
    pipe.source().configureBlocking(false);
    SelectionKey key = pipe.source().register(selector.getSelector(), 0, new SelectionKeyHandler() {
      @Override
      public void modifyKey(SelectionKey k) {
        modified[0]++;
      }
    });
    selector.modifyKey(key);

    selector.doInvocations();
    selector.doInvocations();

    assertEquals(1, modified[0], "a key must be modified once per modifyKey() call, not on every loop");
  }

  @Test
  public void watchdogIsADaemonThatStopsOnDestroy() throws InterruptedException {
    selector = new ProfileSelector("test", new SimpleTimeSource(), logManager, null, Integer.MAX_VALUE);
    assertTrue(selector.watchdog.isDaemon(), "the watchdog must not keep the JVM alive");

    selector.destroy();
    selector.watchdog.join(10000);

    assertFalse(selector.watchdog.isAlive(), "destroy() must stop the watchdog");
  }
}
