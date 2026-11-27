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
package rice.environment.time.simulated;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import rice.environment.Environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DirectTimeSourceTest {

  static final long START = 12345;
  static final long JOIN_MILLIS = 5000;

  @Test
  public void startsAtTheGivenTime() {
    assertEquals(START, new DirectTimeSource(START).currentTimeMillis());
  }

  @Test
  public void nowIsTheVirtualTime() {
    assertEquals(Instant.ofEpochMilli(START), new DirectTimeSource(START).now());
  }

  @Test
  public void setTimeWorksWithoutALogManager() {
    DirectTimeSource directTimeSource = new DirectTimeSource(START);

    directTimeSource.setTime(START + JOIN_MILLIS);
    directTimeSource.setTime(START);

    assertEquals(START + JOIN_MILLIS, directTimeSource.currentTimeMillis(), "the clock must not go back");
  }

  @Test
  public void negativeStartTimeMeansNow() {
    long before = System.currentTimeMillis();
    DirectTimeSource directTimeSource = new DirectTimeSource(-1);
    assertTrue(directTimeSource.currentTimeMillis() >= before, "a negative start time should start the clock at the wall-clock time");
  }

  @Test
  public void destroyWakesUpSleepers() throws InterruptedException {
    Environment env = Environment.directEnvironment();
    try {
      final DirectTimeSource directTimeSource = (DirectTimeSource)env.getTimeSource();
      final AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
      Thread sleeper = new Thread(() -> {
        try {
          directTimeSource.sleep(1000000); // simulated time doesn't advance in this test
        } catch (Throwable throwable) {
          thrown.set(throwable);
        }
      }, "DirectTimeSourceTest sleeper");
      sleeper.setDaemon(true);
      sleeper.start();
      long deadline = System.currentTimeMillis() + JOIN_MILLIS;
      while (sleeper.getState() != Thread.State.WAITING && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(Thread.State.WAITING, sleeper.getState());

      directTimeSource.destroy();

      sleeper.join(JOIN_MILLIS);
      assertFalse(sleeper.isAlive(), "destroy() must wake up the sleeping thread");
      assertInstanceOf(InterruptedException.class, thrown.get());
    } finally {
      env.destroy();
    }
  }

  @Test
  public void sleepOnTheSelectorThreadFailsInsteadOfHanging() throws InterruptedException {
    Environment env = Environment.directEnvironment();
    try {
      final DirectTimeSource directTimeSource = (DirectTimeSource)env.getTimeSource();
      final AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
      env.getSelectorManager().invoke(() -> {
        try {
          directTimeSource.sleep(1000);
        } catch (Throwable throwable) {
          thrown.set(throwable);
        }
      });
      long deadline = System.currentTimeMillis() + JOIN_MILLIS;
      while (thrown.get() == null && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      assertInstanceOf(IllegalStateException.class, thrown.get());
    } finally {
      env.destroy();
    }
  }
}
