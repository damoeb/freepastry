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
    @Override
    public void receiveResult(R result) { thread = Thread.currentThread(); received.add("result:"+result); }
    @Override
    public void receiveException(Exception e) { thread = Thread.currentThread(); received.add(e); }
  }

  @Test
  public void resultCompletesFuture() throws Exception {
    FutureContinuation<String, IOException> futureContinuation = new FutureContinuation<String, IOException>();
    assertFalse(futureContinuation.future().isDone());
    futureContinuation.receiveResult("hello");
    assertEquals("hello", futureContinuation.future().get(1, TimeUnit.SECONDS));
  }

  @Test
  public void exceptionCompletesFutureExceptionally() {
    FutureContinuation<String, IOException> futureContinuation = new FutureContinuation<String, IOException>();
    IOException ioe = new IOException("boom");
    futureContinuation.receiveException(ioe);
    ExecutionException executionException = assertThrows(ExecutionException.class, () -> futureContinuation.future().get());
    assertSame(ioe, executionException.getCause());
  }

  @Test
  public void firstOutcomeWins() {
    FutureContinuation<String, Exception> futureContinuation = new FutureContinuation<String, Exception>();
    futureContinuation.receiveResult("first");
    futureContinuation.receiveException(new Exception("late"));
    futureContinuation.receiveResult("second");
    assertEquals("first", futureContinuation.future().join());
  }

  @Test
  public void nullExceptionStillCompletes() {
    FutureContinuation<String, Exception> futureContinuation = new FutureContinuation<String, Exception>();
    futureContinuation.receiveException(null);
    assertTrue(futureContinuation.future().isCompletedExceptionally());
  }

  @Test
  public void wrapsGivenFuture() {
    CompletableFuture<Integer> completableFuture = new CompletableFuture<Integer>();
    new FutureContinuation<Integer, Exception>(completableFuture).receiveResult(42);
    assertEquals(42, completableFuture.join());
  }

  @Test
  public void callCompletesWhenCallbackFires() throws Exception {
    final List<Continuation<String, Exception>> pending = new ArrayList<Continuation<String, Exception>>();
    CompletableFuture<String> completableFuture = FutureContinuation.call(c -> pending.add(c));
    assertFalse(completableFuture.isDone());
    pending.get(0).receiveResult("later");
    assertEquals("later", completableFuture.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void callCapturesThrowingOperation() {
    IllegalStateException ise = new IllegalStateException("can't start");
    CompletableFuture<String> completableFuture = FutureContinuation.call(c -> { throw ise; });
    CompletionException completionException = assertThrows(CompletionException.class, completableFuture::join);
    assertSame(ise, completionException.getCause());
  }

  @Test
  public void forwardResult() {
    Recorder<String> recorder = new Recorder<String>();
    CompletableFuture<String> completableFuture = new CompletableFuture<String>();
    FutureContinuation.forward(completableFuture, recorder);
    assertTrue(recorder.received.isEmpty());
    completableFuture.complete("x");
    assertEquals(List.of("result:x"), recorder.received);
  }

  @Test
  public void forwardAlreadyCompletedRunsImmediately() {
    Recorder<String> recorder = new Recorder<String>();
    FutureContinuation.forward(CompletableFuture.completedFuture("done"), recorder);
    assertEquals(List.of("result:done"), recorder.received);
  }

  @Test
  public void forwardUnwrapsCompletionException() {
    Recorder<Integer> recorder = new Recorder<Integer>();
    IOException ioe = new IOException("io");
    CompletableFuture<Integer> source = new CompletableFuture<Integer>();
    // a dependent stage wraps the failure in a CompletionException
    FutureContinuation.forward(source.thenApply(i -> i + 1), recorder);
    source.completeExceptionally(ioe);
    assertEquals(1, recorder.received.size());
    assertSame(ioe, recorder.received.get(0));
  }

  @Test
  public void forwardWrapsErrors() {
    Recorder<Integer> recorder = new Recorder<Integer>();
    AssertionError err = new AssertionError("fatal");
    CompletableFuture<Integer> completableFuture = new CompletableFuture<Integer>();
    completableFuture.completeExceptionally(err);
    FutureContinuation.forward(completableFuture, recorder);
    ExecutionException executionException = assertInstanceOf(ExecutionException.class, recorder.received.get(0));
    assertSame(err, executionException.getCause());
  }

  @Test
  public void forwardReportsContinuationFailure() {
    CompletableFuture<String> completableFuture = CompletableFuture.completedFuture("x");
    RuntimeException bug = new RuntimeException("bug in continuation");
    CompletableFuture<Void> done = FutureContinuation.forward(completableFuture, new Continuation<String, Exception>() {
      @Override
      public void receiveResult(String result) { throw bug; }
      @Override
      public void receiveException(Exception exception) { fail("unexpected"); }
    }).toCompletableFuture();
    CompletionException completionException = assertThrows(CompletionException.class, done::join);
    assertSame(bug, completionException.getCause());
  }

  @Test
  public void forwardViaExecutor() throws Exception {
    final List<Runnable> queued = new ArrayList<Runnable>();
    Executor executor = queued::add; // stands in for selectorManager::invoke
    Recorder<String> recorder = new Recorder<String>();
    CompletableFuture<String> completableFuture = new CompletableFuture<String>();
    FutureContinuation.forward(completableFuture, recorder, executor);
    completableFuture.complete("queued");
    assertTrue(recorder.received.isEmpty(), "must not run before the executor runs it");
    assertEquals(1, queued.size());
    queued.get(0).run();
    assertEquals(List.of("result:queued"), recorder.received);
  }

  @Test
  public void roundTrip() throws Exception {
    // Continuation -> future -> Continuation
    Recorder<String> recorder = new Recorder<String>();
    FutureContinuation<String, Exception> futureContinuation = new FutureContinuation<String, Exception>();
    FutureContinuation.forward(futureContinuation.future().thenApply(String::toUpperCase), recorder);
    futureContinuation.receiveResult("abc");
    assertEquals(List.of("result:ABC"), recorder.received);
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
