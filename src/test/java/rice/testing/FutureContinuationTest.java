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
package rice.testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import rice.Continuation;
import rice.FutureContinuation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests the CompletableFuture interop of {@link FutureContinuation}.
 */
public class FutureContinuationTest {

  /** Records what a Continuation receives. */
  static class Recorder<R> implements Continuation<R, Exception> {
    final List<Object> received = new ArrayList<Object>();
    Thread thread;
    public void receiveResult(R result) { thread = Thread.currentThread(); received.add("result:"+result); }
    public void receiveException(Exception e) { thread = Thread.currentThread(); received.add(e); }
  }

  @Test
  public void resultCompletesFuture() throws Exception {
    FutureContinuation<String, IOException> c = new FutureContinuation<String, IOException>();
    assertFalse(c.future().isDone());
    c.receiveResult("hello");
    assertEquals("hello", c.future().get(1, TimeUnit.SECONDS));
  }

  @Test
  public void exceptionCompletesFutureExceptionally() {
    FutureContinuation<String, IOException> c = new FutureContinuation<String, IOException>();
    IOException ioe = new IOException("boom");
    c.receiveException(ioe);
    ExecutionException ee = assertThrows(ExecutionException.class, () -> c.future().get());
    assertSame(ioe, ee.getCause());
  }

  @Test
  public void firstOutcomeWins() {
    FutureContinuation<String, Exception> c = new FutureContinuation<String, Exception>();
    c.receiveResult("first");
    c.receiveException(new Exception("late"));
    c.receiveResult("second");
    assertEquals("first", c.future().join());
  }

  @Test
  public void nullExceptionStillCompletes() {
    FutureContinuation<String, Exception> c = new FutureContinuation<String, Exception>();
    c.receiveException(null);
    assertTrue(c.future().isCompletedExceptionally());
  }

  @Test
  public void wrapsGivenFuture() {
    CompletableFuture<Integer> f = new CompletableFuture<Integer>();
    new FutureContinuation<Integer, Exception>(f).receiveResult(42);
    assertEquals(42, f.join());
  }

  @Test
  public void callCompletesWhenCallbackFires() throws Exception {
    final List<Continuation<String, Exception>> pending = new ArrayList<Continuation<String, Exception>>();
    CompletableFuture<String> f = FutureContinuation.call(c -> pending.add(c));
    assertFalse(f.isDone());
    pending.get(0).receiveResult("later");
    assertEquals("later", f.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void callCapturesThrowingOperation() {
    IllegalStateException ise = new IllegalStateException("can't start");
    CompletableFuture<String> f = FutureContinuation.call(c -> { throw ise; });
    CompletionException ce = assertThrows(CompletionException.class, f::join);
    assertSame(ise, ce.getCause());
  }

  @Test
  public void forwardResult() {
    Recorder<String> r = new Recorder<String>();
    CompletableFuture<String> f = new CompletableFuture<String>();
    FutureContinuation.forward(f, r);
    assertTrue(r.received.isEmpty());
    f.complete("x");
    assertEquals(List.of("result:x"), r.received);
  }

  @Test
  public void forwardAlreadyCompletedRunsImmediately() {
    Recorder<String> r = new Recorder<String>();
    FutureContinuation.forward(CompletableFuture.completedFuture("done"), r);
    assertEquals(List.of("result:done"), r.received);
  }

  @Test
  public void forwardUnwrapsCompletionException() {
    Recorder<Integer> r = new Recorder<Integer>();
    IOException ioe = new IOException("io");
    CompletableFuture<Integer> source = new CompletableFuture<Integer>();
    // a dependent stage wraps the failure in a CompletionException
    FutureContinuation.forward(source.thenApply(i -> i + 1), r);
    source.completeExceptionally(ioe);
    assertEquals(1, r.received.size());
    assertSame(ioe, r.received.get(0));
  }

  @Test
  public void forwardWrapsErrors() {
    Recorder<Integer> r = new Recorder<Integer>();
    AssertionError err = new AssertionError("fatal");
    CompletableFuture<Integer> f = new CompletableFuture<Integer>();
    f.completeExceptionally(err);
    FutureContinuation.forward(f, r);
    ExecutionException ee = assertInstanceOf(ExecutionException.class, r.received.get(0));
    assertSame(err, ee.getCause());
  }

  @Test
  public void forwardReportsContinuationFailure() {
    CompletableFuture<String> f = CompletableFuture.completedFuture("x");
    RuntimeException bug = new RuntimeException("bug in continuation");
    CompletableFuture<Void> done = FutureContinuation.forward(f, new Continuation<String, Exception>() {
      public void receiveResult(String result) { throw bug; }
      public void receiveException(Exception exception) { fail("unexpected"); }
    }).toCompletableFuture();
    CompletionException ce = assertThrows(CompletionException.class, done::join);
    assertSame(bug, ce.getCause());
  }

  @Test
  public void forwardViaExecutor() throws Exception {
    final List<Runnable> queued = new ArrayList<Runnable>();
    Executor executor = queued::add; // stands in for selectorManager::invoke
    Recorder<String> r = new Recorder<String>();
    CompletableFuture<String> f = new CompletableFuture<String>();
    FutureContinuation.forward(f, r, executor);
    f.complete("queued");
    assertTrue(r.received.isEmpty(), "must not run before the executor runs it");
    assertEquals(1, queued.size());
    queued.get(0).run();
    assertEquals(List.of("result:queued"), r.received);
  }

  @Test
  public void roundTrip() throws Exception {
    // Continuation -> future -> Continuation
    Recorder<String> r = new Recorder<String>();
    FutureContinuation<String, Exception> c = new FutureContinuation<String, Exception>();
    FutureContinuation.forward(c.future().thenApply(String::toUpperCase), r);
    c.receiveResult("abc");
    assertEquals(List.of("result:ABC"), r.received);
  }

  @Test
  public void toException() {
    Exception e = new Exception("plain");
    assertSame(e, FutureContinuation.toException(e));
    assertSame(e, FutureContinuation.toException(new CompletionException(new ExecutionException(e))));
    CompletionException noCause = new CompletionException("no cause", null);
    assertSame(noCause, FutureContinuation.toException(noCause));
  }
}
