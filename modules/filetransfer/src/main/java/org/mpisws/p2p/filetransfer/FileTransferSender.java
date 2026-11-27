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
package org.mpisws.p2p.filetransfer;

import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CANCEL;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CANCEL_REQUEST;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.mpisws.p2p.transport.util.BoundedPriorityWriteQueue;
import org.slf4j.Logger;
import rice.Continuation;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.selector.SelectorLoop;

/**
 * The sending half of a {@link FileTransferImpl}: the outgoing transfers (messages and files, each cut into chunks), the
 * prioritised queue of what is to be written to the socket, and the cancel messages.
 *
 * The queue is the only thing that other threads may touch (through {@code sendMessage} and {@code sendFile}); everything
 * else runs on the selector thread.
 */
class FileTransferSender {
  final FileTransferImpl transfer;
  final Logger logger;
  private final SelectorLoop selectorManager;
  /** Set when the transfer failed, guarded by the queue's monitor. */
  private boolean failed = false;

  FileTransferSender(FileTransferImpl transfer) {
    this.transfer = transfer;
    this.logger = transfer.logger;
    this.selectorManager = transfer.environment.getSelectorManager();
    this.queue = new BoundedPriorityWriteQueue<MessageWrapper>(transfer.MAX_QUEUE_SIZE, MessageWrapper::drop, logger);
  }

  /** @return whether the transfer failed already */
  boolean hasFailed() {
    synchronized(queue) {
      return failed;
    }
  }

  /** Marks the transfer as failed and drops what waits in the queue. */
  void dropQueuedMessages() {
    Iterable<MessageWrapper> dropMe;
    synchronized(queue) {
      failed = true;
      dropMe = queue.waiting();
    }
    for (MessageWrapper foo : dropMe) {
      foo.drop();
    }
  }

  /** Fails every outgoing transfer that is not complete. */
  void failOutgoingTransfers() {
    for (OutgoingReceipt receiptImpl : new ArrayList<OutgoingReceipt>(outgoingData.values())) {
      receiptImpl.failed();
    }
  }

  /** Writes the queued messages to the socket, as long as it takes them. */
  void writeQueued(AppSocket socket) throws IOException {
    registered = false;
    MessageWrapper current = queue.poll();
    while (current != null && current.receiveSelectResult(socket)) {
      current = queue.poll();
    }
    scheduleToWriteIfNeeded();
  }

  private int seq = Integer.MIN_VALUE;
  private final Object uidLock = new Object();
  final BoundedPriorityWriteQueue<MessageWrapper> queue; // messages we want to send, and the one being written
  boolean registered = false;

  void enqueue(MessageWrapper ret) {
    synchronized(queue) {
      if (failed) return;
      queue.add(ret);
    }

    if (selectorManager.isSelectorThread()) {
      scheduleToWriteIfNeeded();
    } else {
      selectorManager.invoke(() -> scheduleToWriteIfNeeded());
    }
  }

  /**
   * Must be called on selectorManager.
   *
   * A) finds a writingSocket if possible
   *   opens one if needed
   */
  void scheduleToWriteIfNeeded() {
    if (!selectorManager.isSelectorThread()) throw new IllegalStateException("Must be called on the selector");

    // register on the writingSocket if needed
    if (!registered) {
      if (haveMessageToSend()) {
        // maybe we should remember if we were registered, and don't reregister, but for now it doesn't hurt
        registered = true;  // may fail in this call and set registered back to false, so make sure to do this before calling register
        logger.trace("{}.scheduleToWriteIfNeeded() registering to write", this);
        transfer.socket.register(false, true, 300000, transfer);
      }
    }
  }

  /**
   * True if we have a message to send
   */
  private boolean haveMessageToSend() {
    return !queue.isEmpty();
  }

  @SuppressWarnings("ReferenceEquality") // the same instance is meant
  boolean complete(MessageWrapper wrapper) {
    logger.trace("{}.complete({})", this, wrapper);
    if (wrapper != queue.inFlight()) throw new IllegalArgumentException("Wrapper:"+wrapper+" messageThatIsBeingWritten:"+queue.inFlight());

    queue.clearInFlight();

    // notify deliverAckToMe
    wrapper.complete();

    // close the socket if we need to
    return true;
  }

  /**
   * @return the length of the file, or 0 if it does not exist or cannot be read, as File.length() did
   */
  static long length(Path path) {
    try {
      return Files.size(path);
    } catch (IOException e) {
      return 0;
    }
  }

  FileReceipt sendFile(Path path, ByteBuffer metadataBB, byte priority, long offset, long length,
      Continuation<FileReceipt, Exception> continuation) throws IOException {
    if (path == null || !Files.exists(path) || Files.isDirectory(path)) throw new IllegalArgumentException("File f must be non-null, exist, and must not be a directory. "+path);
    byte[] metadata = new byte[metadataBB.remaining()];
    metadataBB.get(metadata);
    OutgoingFileReceipt ret = new OutgoingFileReceipt(this, path,metadata,priority,offset,length,allocateTransferUid(),continuation);
    return ret;
  }

  ByteBufferReceipt sendMessage(ByteBuffer buffer, byte priority,
      Continuation<ByteBufferReceipt, Exception> continuation) {
    if (buffer == null) throw new IllegalArgumentException("ByteBuffer bb must be non-null");
    OutgoingMessageReceipt ret = new OutgoingMessageReceipt(this, buffer,priority,allocateTransferUid(),continuation);
    return ret;
  }

  int allocateTransferUid() {
    synchronized (uidLock) {
      return seq++;
    }
  }

  Map<Integer, OutgoingReceipt> outgoingData = new HashMap<Integer, OutgoingReceipt>();




  void receiverCancelled(int uid) {
    OutgoingReceipt writer = outgoingData.remove(uid);
    if (writer != null) {
      writer.cancel();
      writer.notifyReceiverCancelled();
      transfer.notifyListenersReceiverCancelled(writer);
    } else {
      logger.warn("receiverCanclled({}) no record of the uid.", uid);
    }
  }



  /**
   * Only call this on the receiver side, b/c uid is simplex
   * @return if we can guarantee the request was cancelled
   */
  boolean requestCancel(int uid) {
    enqueue(new SimpleMessageWrapper(this, MSG_CANCEL_REQUEST, uid));
    return false;
  }

  /**
   * Only call this on the sender side, b/c uid is simplex
   */
  boolean sendCancel(int uid) {
    enqueue(new SimpleMessageWrapper(this, MSG_CANCEL, uid));
    return false;
  }
}
