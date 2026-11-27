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
/*
 * Created on Aug 9, 2005
 */
package rice.environment.processing.simple;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;

import rice.Executable;
import rice.Futures;
import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.processing.Processor;
import rice.environment.processing.WorkRequest;
import rice.environment.time.TimeSource;
import rice.selector.SelectorLoop;

/**
 * @author Jeff Hoye
 */
public class SimpleProcessor implements Processor {
  // the queue used for processing requests
  private final PriorityBlockingQueue<ProcessingRequest<?>> QUEUE;
  private final ProcessingThread THREAD;

  // for blocking IO WorkRequests
  private final WorkQueue workQueue;
  protected BlockingIOThread bioThread;

  long seq = Long.MIN_VALUE;

  @SuppressWarnings("ThreadPriorityCheck") // a hint only: CPU-heavy work must not starve the selector
  public SimpleProcessor(String name) {
    QUEUE = new PriorityBlockingQueue<ProcessingRequest<?>>();
    THREAD = new ProcessingThread("FreePastry Processing -- " + name, QUEUE);
    THREAD.start();
    THREAD.setPriority(Thread.MIN_PRIORITY);
    workQueue = new WorkQueue();
    bioThread = new BlockingIOThread("FreePastry Blocking IO -- " + name, workQueue);
    bioThread.start();
  }

  /**
   * Schedules a job for processing on the dedicated processing thread. CPU
   * intensive jobs, such as encryption, erasure encoding, or bloom filter
   * creation should never be done in the context of the underlying node's
   * thread, and should only be done via this method.
   *
   * @param task
   *          The task to run on the processing thread
   * @return completed on the selector thread; cancelling it before the task starts keeps it from running
   */
  @Override
  public <R> CompletableFuture<R> process(Executable<R, ? extends Exception> task,
      SelectorLoop selector, TimeSource timeSource, LogManager log) {
    return process(task, 0, selector, timeSource, log);
  }

  public <R> CompletableFuture<R> process(Executable<R, ? extends Exception> task, int priority,
      SelectorLoop selector, TimeSource timeSource, LogManager log) {
    long nextSeq;
    synchronized(SimpleProcessor.this) {
      nextSeq = seq++;
    }
    ProcessingRequest<R> request = new ProcessingRequest<R>(task, priority, nextSeq, log, timeSource, selector);
    QUEUE.offer(request);
    return Futures.onCancel(request.future(), request);
  }

  @Override
  public <R> CompletableFuture<R> processBlockingIO(Callable<R> work, SelectorLoop selector) {
    WorkRequest<R> request = new WorkRequest<R>(work, selector);
    workQueue.enqueue(request);
    return Futures.onCancel(request.future(), request);
  }

  @Override
  public void destroy() {
    THREAD.shutdown();
    QUEUE.clear();
    bioThread.shutdown();
    workQueue.destroy();
  }

  /**
   * This is a test to make sure the order is correct.
   */
  @SuppressWarnings("NullAway") // the demo tasks have no result
  public static void main(String[] args) throws Exception {
    Environment env = new Environment();
    Processor processor = env.getProcessor();
    // block the processor for 1 second while we schedule some more stuff
    Futures.reportUnhandled(processor.process(() -> {
      try { Thread.sleep(1000); } catch (InterruptedException ignored) { /* only delays the demo */ }
      return null;
    }, env.getSelectorManager(), env.getTimeSource(), env.getLogManager())
        .thenRun(() -> System.out.println("Done blocking.")), env);

    for (int seq = 0; seq < 10; seq++) {
      final int mySeq = seq;
      Futures.reportUnhandled(processor.process(() -> {
        System.out.println("Executed Seq: "+mySeq);
        return null;
      }, env.getSelectorManager(), env.getTimeSource(), env.getLogManager())
          .thenRun(() -> System.out.println("Received Seq: "+mySeq)), env);
      System.out.println("Done scheduling "+mySeq);
    }
  }
}
