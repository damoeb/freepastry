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

package rice.persistence.testing;

/*
 * @(#) StorageTest.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 *
 * @version $Id: Test.java 3613 2007-02-15 14:45:14Z jstewart $
 */
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import rice.environment.Environment;

/**
 * This class is a class which tests the Storage class
 * in the rice.persistence package.
 */
public abstract class Test {

  protected static final String SUCCESS = "SUCCESS";
  protected static final String FAILURE = "FAILURE";

  protected static final int PAD_SIZE = 60;

  public abstract void start();

  protected static final long MAIN_TIMEOUT_MILLIS = 120000;

  protected Environment environment;

  private final CountDownLatch finished = new CountDownLatch(1);
  private AssertionError failure;
  private volatile String currentStep;

  public Test(Environment env) {
    environment = env;
  }

  protected void sectionStart(String name) {
    System.out.println(name);
  }

  protected void sectionEnd() {
    System.out.println();
  }

  protected void stepStart(String name) {
    currentStep = name;
    System.out.print(pad("  " + name));
  }

  protected void stepDone(String status) {
    System.out.println("[" + status + "]");
    if(status.equals(FAILURE))
      fail(new AssertionError("Step failed: " + currentStep));
  }

  protected void stepDone(String status, String message) {
    System.out.println("[" + status + "]");
    System.out.println("    " + message);
    if(status.equals(FAILURE))
      fail(new AssertionError("Step failed: " + currentStep + ": " + message));
  }

  protected void stepException(Exception e) {
    System.out.println();

    System.out.println("Exception " + e + " occurred during testing.");

    e.printStackTrace();
    fail(new AssertionError("Exception during step: " + currentStep, e));
  }

  /**
   * Records the first failure, releases {@link #await(long)} and throws the error to stop the
   * continuation chain (the steps used to call System.exit(0), even on failure).
   */
  private void fail(AssertionError error) {
    synchronized (finished) {
      if (failure == null) failure = error;
    }
    finished.countDown();
    throw error;
  }

  /**
   * Called by a subclass at the end of its last step.
   */
  protected void testsPassed() {
    System.out.println("All tests completed successfully.");
    finished.countDown();
  }

  /**
   * Waits until the steps started by {@link #start()} are done.
   *
   * @throws AssertionError if a step failed, or the steps did not finish within the timeout
   */
  public void await(long timeoutMillis) throws InterruptedException {
    if (!finished.await(timeoutMillis, TimeUnit.MILLISECONDS))
      throw new AssertionError("Steps did not finish within " + timeoutMillis + " ms, last step: " + currentStep);
    synchronized (finished) {
      if (failure != null) throw failure;
    }
  }

  /**
   * Runs the test as a program: exits with 0 if all steps passed, 1 otherwise.
   */
  protected static void runMain(Test test) {
    int status = 1;
    try {
      test.start();
      test.await(MAIN_TIMEOUT_MILLIS);
      status = 0;
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      test.environment.destroy();
    }
    System.exit(status);
  }

  private String pad(String start) {
    if (start.length() >= PAD_SIZE) {
      return start.substring(0, PAD_SIZE);
    } else {
      int spaceLength = PAD_SIZE - start.length();
      char[] spaces = new char[spaceLength];
      Arrays.fill(spaces, '.');

      return start.concat(new String(spaces));
    }
  }
}
