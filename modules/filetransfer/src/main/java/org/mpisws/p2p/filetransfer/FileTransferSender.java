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

import static org.mpisws.p2p.filetransfer.FileTransferImpl.CANCEL_PRIORITY;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_BB_HEADER;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CANCEL;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CANCEL_REQUEST;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CHUNK;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_FILE_HEADER;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.util.BoundedPriorityWriteQueue;
import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.processing.WorkRequest;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.util.MathUtils;
import rice.selector.SelectorLoop;

/**
 * The sending half of a {@link FileTransferImpl}: the outgoing transfers (messages and files, each cut into chunks), the
 * prioritised queue of what is to be written to the socket, and the cancel messages.
 *
 * The queue is the only thing that other threads may touch (through {@code sendMessage} and {@code sendFile}); everything
 * else runs on the selector thread.
 */
class FileTransferSender {
  private final FileTransferImpl transfer;
  private final Logger logger;
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
    for (ReceiptImpl receiptImpl : new ArrayList<ReceiptImpl>(outgoingData.values())) {
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

  private void enqueue(MessageWrapper ret) {
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
    FileReceiptImpl ret = new FileReceiptImpl(path,metadata,priority,offset,length,allocateTransferUid(),continuation);
    return ret;
  }

  ByteBufferReceipt sendMessage(ByteBuffer buffer, byte priority,
      Continuation<ByteBufferReceipt, Exception> continuation) {
    if (buffer == null) throw new IllegalArgumentException("ByteBuffer bb must be non-null");
    BBReceiptImpl ret = new BBReceiptImpl(buffer,priority,allocateTransferUid(),continuation);
    return ret;
  }

  int allocateTransferUid() {
    synchronized (uidLock) {
      return seq++;
    }
  }

  /**
   * Will Schedule MessageWrappers as needed
   *
   * So, a File or BB corresponds 1 to 1 for ReceiptImpls, but a ReceiptImpl will schedule
   * several MessageWrappers for writing.
   *
   * @author Jeff Hoye
   *
   */
  abstract class ReceiptImpl implements Receipt {
    byte priority;
    int uid;
    boolean cancelled = false;
    boolean completed = false;

    public ReceiptImpl(byte priority, int uid) {
      this.priority = priority;
      this.uid = uid;
      outgoingData.put(uid, this);
    }

    @Override
    public byte getPriority() {
      return priority;
    }

    @Override
    public int getUid() {
      return uid;
    }

    void failed() {
      transfer.notifyListenersTransferFailed(this, false);
      cancelled = true;
      outgoingData.remove(uid);
    }

    public boolean isCancelled() {
      return cancelled;
    }

    abstract void complete(MessageWrapper wrapper);

    @Override
    public boolean cancel() {
      cancelled = true;
      outgoingData.remove(uid);
      sendCancel(uid);
      return !completed;
    }

    abstract public void notifyReceiverCancelled();
  }

  Map<Integer, ReceiptImpl> outgoingData = new HashMap<Integer, ReceiptImpl>();




  /**
   * Each time complete is called, schedule the next one chunk.
   *
   * @author Jeff Hoye
   */
  class BBReceiptImpl extends ReceiptImpl implements ByteBufferReceipt {
    /**
     * The whole message to send.
     */
    ByteBuffer msg;
    List<ByteBuffer> msgList;
    ByteBuffer header;
    MessageWrapperImpl outstanding; // = new MessageWrapper
    int wrapperSeq = Integer.MIN_VALUE+10; // to give cancel priority
    /**
     * The purpose of chunkBuffer is to limit the amount of data being sent
     * for a single message.  So this is an 8K window that slides along the message
     */
    final ByteBuffer chunkBuffer;
    int initialPosition;
    Continuation<ByteBufferReceipt, Exception> deliverAckToMe;

    public BBReceiptImpl(ByteBuffer buffer, byte priority, int uid, Continuation<ByteBufferReceipt, Exception> continuation) {
      super(priority, uid);
      this.deliverAckToMe = continuation;
      this.msg = buffer;
      initialPosition = msg.position();
      msgList = new ArrayList<ByteBuffer>();
      // shares msg's content, position and limit, whatever its arrayOffset()
      chunkBuffer = msg.duplicate();

      // construct header
      // byte MSG_BB_HEADER, int UID, int length
      header = ByteBuffer.allocate(9);
      header.put(MSG_BB_HEADER);
      header.put(MathUtils.intToByteArray(uid));
      header.put(MathUtils.intToByteArray(buffer.remaining()));
      header.clear();
      msgList.add(header);

      outstanding = new MessageWrapperImpl(this,wrapperSeq++,msgList);
      enqueue(outstanding);
    }

    @Override
    public String toString() {
      return "Outgoing msg<"+uid+"> size:"+getSize()+" priority:"+priority+" msg:"+msg;
    }

    @Override
    void failed() {
      if (deliverAckToMe != null) deliverAckToMe.receiveException(new TransferFailedException(this));
      super.failed();
    }

    @Override
    void complete(MessageWrapper wrapper) {
      // advance msg's pointer as necessary
        msg.position(chunkBuffer.position());

      // notify listener
      transfer.notifyListenersSendMsgProgress(this,msg.position()-initialPosition, msg.limit()-initialPosition);

      // if need to send more:
      if (msg.hasRemaining()) {
        // Construct a chunk (note that we reuse all the objects...)
        // Construct a BB of the right size
        if (msg.remaining() > transfer.CHUNK_SIZE) {
          // don't send the whole message
          chunkBuffer.limit(msg.position()+transfer.CHUNK_SIZE);
        } else {
          // send the whole message
          chunkBuffer.limit(msg.limit());
        }

        header.clear();
        header.put(MSG_CHUNK);
        header.put(MathUtils.intToByteArray(uid));
        header.put(MathUtils.intToByteArray(chunkBuffer.remaining()));
        header.clear();
        msgList.add(header);
        msgList.add(chunkBuffer);

        outstanding.clear(msgList, wrapperSeq++);

        // schedule it
        enqueue(outstanding);
        return;
      }

      // we've sent the whole message
      outgoingData.remove(uid);
      if (deliverAckToMe != null) deliverAckToMe.receiveResult(this);
      completed = true;
    }

    @Override
    public byte[] getBytes() {
      byte[] bytes = new byte[msg.limit()-initialPosition];
      ByteBuffer all = msg.duplicate();
      all.position(initialPosition);
      all.get(bytes);
      return bytes;
    }


    @Override
    public long getSize() {
      return (long)msg.limit()-initialPosition;
    }

    @Override
    public boolean cancel() {
      outstanding.cancel();
      return super.cancel();
    }

    @Override
    public void notifyReceiverCancelled() {
      if (deliverAckToMe != null) deliverAckToMe.receiveException(new OperationCancelledException(this));
    }
  }

  /**
   * Keep up to FILE_CACHE of chunks scheduled, and every time one finishes, read some more to schedule more.
   *
   * TODO: Make more than 1 outstanding chunk
   *
   * @author Jeff Hoye
   *
   */
  class FileReceiptImpl extends ReceiptImpl implements FileReceipt {
    /**
     * The whole message to send.
     */
    InputStream file;

    Path f;
    byte[] metadata;
    Continuation<FileReceipt, Exception> deliverAckToMe;

    List<MessageWrapperImpl> outstanding = new ArrayList<MessageWrapperImpl>();// = new MessageWrapper
    List<WorkRequest<Object>> outstandingWRs = new ArrayList<WorkRequest<Object>>();
    int wrapperSeq = Integer.MIN_VALUE;
    long lastByte;
    long ptr; // managed on the selector thread
    long length;
    long initialPosition;


    public FileReceiptImpl(Path path, byte[] metadata, byte priority, long offset, long length, int uid, Continuation<FileReceipt, Exception> continuation) throws IOException {
      super(priority, uid);
      if (offset+length > length(path)) throw new IllegalArgumentException("File is only "+length(path)+" but you are trying to send "+length+" bytes starting at "+offset);
      this.f = path;
      this.metadata = metadata;
      try {
        this.file = Files.newInputStream(path);
      } catch (IOException | RuntimeException | Error e) {
        failed();
        throw e;
      }
      file.skip(offset);
      lastByte = offset+length;

      this.deliverAckToMe = continuation;
      this.initialPosition = offset;
      this.ptr = offset;
      this.length = length;


      ArrayList<ByteBuffer> msgList = new ArrayList<ByteBuffer>();
      // construct header
      // byte MSG_BB_HEADER, int UID, long offset, long length, int nameLength, UTF name
      ByteBuffer header = ByteBuffer.allocate(25);
      header.put(MSG_FILE_HEADER);
      header.put(MathUtils.intToByteArray(uid));
      header.put(MathUtils.longToByteArray(offset));
      header.put(MathUtils.longToByteArray(length));
      header.put(MathUtils.intToByteArray(metadata.length));
      header.clear();
      msgList.add(header);
      msgList.add(ByteBuffer.wrap(metadata));

      MessageWrapperImpl foo = new MessageWrapperImpl(this,wrapperSeq++,msgList);
      enqueue(foo);

      // NOTE: the last one comes when foo is written
      for (int i = 0; i < transfer.NUM_OUTSTANDING_CHUNKS_TO_WRITE_TO_NETWORK-1; i++) {
        scheduleNewFileReaderIfNecessary();
      }
    }

    @Override
    void failed() {
      if (deliverAckToMe != null) deliverAckToMe.receiveException(new TransferFailedException(this));
      super.failed();
    }

    @Override
    public String toString() {
      return "Outgoing file<"+uid+"> "+metadata.length+" size:"+getSize()+" priority:"+priority+" "+f;
    }

    public void scheduleNewFileReaderIfNecessary() {
      // call me on the selector

      if (cancelled) return;

      // if need to send more:
      if (ptr < lastByte) {
        // Construct a chunk (note that we reuse all the objects...)

        final int amtToRead = (int)Math.min(transfer.CHUNK_SIZE,lastByte-ptr);
        final long seq = wrapperSeq++;

        ptr+=amtToRead;
        Continuation<Object, Exception> c = new Continuation<Object, Exception>() {

          @Override
          public void receiveResult(Object result) {
            // this is handled in MessageWrapperImpl.complete()

          }

          @Override
          public void receiveException(Exception exception) {
            if (deliverAckToMe != null) deliverAckToMe.receiveException(exception);
            FileTransferSender.this.sendCancel(uid);
          }

        };

        transfer.processor.processBlockingIO(new WorkRequest<Object>(c, transfer.environment.getSelectorManager()) {

          @Override
          public Object doWork() throws Exception {
            byte[] chunkBytes = new byte[1+4+4+amtToRead];

            chunkBytes[0] = MSG_CHUNK;
            MathUtils.intToByteArray(uid,chunkBytes,1);
            MathUtils.intToByteArray(amtToRead,chunkBytes,5);

            // blocking
            long ret = file.read(chunkBytes,9,amtToRead);
            if (ret < 0) {
              throw new EOFException("Unexpected EOF... cancelling "+uid+" "+f+".");
            }

            ArrayList<ByteBuffer> foo = new ArrayList<ByteBuffer>();
            foo.add(ByteBuffer.wrap(chunkBytes));
            enqueue(new MessageWrapperImpl(FileReceiptImpl.this,seq,foo));
            return null;
          }
        });

        if (ptr == lastByte) {
          // schedule close
          scheduleClose();
        }
      }
    }

    boolean closing = false;
    private final Object closingLock = new Object();
    void scheduleClose() {
      synchronized (closingLock) {
        if (closing) return;
        closing = true;
      }
      Continuation<Object, Exception> c2 = new Continuation<Object, Exception>() {

        @Override
        public void receiveResult(Object result) {
          outgoingData.remove(uid);
          if (deliverAckToMe != null) deliverAckToMe.receiveResult(FileReceiptImpl.this);
        }

        @Override
        public void receiveException(Exception exception) {
          logger.warn("Error closing file <{}> {} {}", uid, file, metadata.length, exception);
        }

      };
      transfer.processor.processBlockingIO(new WorkRequest<Object>(c2, transfer.environment.getSelectorManager()) {
        @Override
        public Object doWork() throws Exception {
          file.close();
          return null;
        }
      });
    }

    @Override
    void complete(MessageWrapper wrapper) {
      // notify listener
      transfer.notifyListenersSendFileProgress(this,ptr-initialPosition, length);

      // called on the selector, no worries about cancelled, it will work out
      if (cancelled) return;

      if (length == 0) {
        // there is no chunk to read, the header was all of it
        scheduleClose();
        return;
      }
      scheduleNewFileReaderIfNecessary();
    }

    @Override
    public long getSize() {
      return length;
    }

    @Override
    public Path getFile() {
      return f;
    }

    @Override
    public ByteBuffer getMetadata() {
      return ByteBuffer.wrap(metadata);
    }

    @Override
    public long getOffset() {
      return initialPosition;
    }

    @Override
    public boolean cancel() {
      cancelled = true;

      for (WorkRequest<Object> workRequest : outstandingWRs) {
        workRequest.cancel();
      }
      for (MessageWrapperImpl mri : outstanding) {
        mri.cancel();
      }

      scheduleClose();

      // this is weird, but we have to run a message through the
      // processor to make sure that cancel is called after all the pending
      // requests are complete
      Continuation<Object, Exception> c = new Continuation<Object, Exception>() {

        @Override
        public void receiveResult(Object result) {
          for (WorkRequest<Object> wr : outstandingWRs) {
            wr.cancel();
          }
          for (MessageWrapperImpl mri : outstanding) {
            mri.cancel();
          }

          FileReceiptImpl.this.superCancel();
        }

        @Override
        public void receiveException(Exception exception) {

        }
      };

      transfer.processor.processBlockingIO(new WorkRequest<Object>(c,transfer.environment.getSelectorManager()) {

        @Override
        public Object doWork() throws Exception {
          // do nothing, just used to call the above continuation after flushing out any existing stuff in the processor
          return null;
        }

      });
      return true; // this is a lie, we don't know for sure whether or not we successfully cancelled
    }

    protected void superCancel() {
      super.cancel();
    }


    @Override
    public void notifyReceiverCancelled() {
      if (deliverAckToMe != null) deliverAckToMe.receiveException(new OperationCancelledException(this));
    }

  }


  interface MessageWrapper extends Comparable<MessageWrapper> {
    byte getPriority();
    void complete();
    void drop();
    int getUid();
    long getSeq();
    boolean receiveSelectResult(AppSocket socket) throws IOException;
  }

  class MessageWrapperImpl implements MessageWrapper {
    boolean started = false;
    ReceiptImpl receipt;
    List<ByteBuffer> message;
    long seq;  // the sequence inside of the ReceiptImpl

    public MessageWrapperImpl(ReceiptImpl receipt, long seq, List<ByteBuffer> message) {
      this.receipt = receipt;
      this.seq = seq;
      this.message = message;
    }

    @Override
    public String toString() {
      return "Part "+seq+" of "+receipt;
    }

    /**
     * Called due to a queue overflow.
     */
    @Override
    public void drop() {
      receipt.failed();
    }

    public boolean cancel() {
      if (this.equals(queue.inFlight())) {
        if (!started) {
          // nothing was written yet, so the message can still be cancelled: its receipt is cancelled too, and when the
          // socket calls back, receiveSelectResult() sees that and drops the message without writing it
          return true;
        } else {
          return false;
        }
      }
      return queue.remove(this);
    }

    /**
     * To make this reusable
     */
    public void clear(List<ByteBuffer> message, long seq) {
      started = false;
      this.message = message;
      this.seq = seq;
    }

    /**
     * Defines the ordering in the queue
     */
    @Override
    public int compareTo(MessageWrapper that) {
      if (this.receipt.priority == that.getPriority()) {
        if (this.receipt.uid == that.getUid()) {
          return Long.compare(this.seq, that.getSeq());
        }
        return this.receipt.uid-that.getUid();
      }
      return this.receipt.priority-that.getPriority();
    }

    /**
     * @return true if should keep writing
     */
    @Override
    public boolean receiveSelectResult(AppSocket socket) throws IOException {
      logger.trace("{}.receiveSelectResult({})", this, socket);

      if (receipt.isCancelled() && !started) {
        logger.trace("{}.rsr({}) cancelled", this, socket);
        queue.clearInFlight();
        return true;
      } else {
        started = true;
        long bytesWritten;
        if ((bytesWritten = socket.write(message.get(0))) == -1) {
          // socket was closed, panic
          transfer.socketClosed();
          return false;
        }

        if (message.get(0).hasRemaining()) {
          logger.trace("{}.rsr({}) has remaining", this, socket);
          return false;
        } else {
          // write the next BB
          message.remove(0);
          if (!message.isEmpty())
            return receiveSelectResult(socket);
        }
      }

      // notify that this MsgWrapper is complete
      return FileTransferSender.this.complete(this);
    }

    @Override
    public void complete() {
      receipt.complete(this);
    }

    @Override
    public byte getPriority() {
      return receipt.getPriority();
    }

    @Override
    public long getSeq() {
      return seq;
    }

    @Override
    public int getUid() {
      return receipt.getUid();
    }
  }

  void receiverCancelled(int uid) {
    ReceiptImpl writer = outgoingData.remove(uid);
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
    enqueue(new SimpleMessageWrapper(MSG_CANCEL_REQUEST, uid));
    return false;
  }

  /**
   * Only call this on the sender side, b/c uid is simplex
   */
  boolean sendCancel(int uid) {
    enqueue(new SimpleMessageWrapper(MSG_CANCEL, uid));
    return false;
  }

  class SimpleMessageWrapper implements MessageWrapper {
    ByteBuffer msg;
    int uid;
    byte msgType;
    public SimpleMessageWrapper(byte msgType, int uid) {
      this.uid = uid;
      this.msgType = msgType;
      msg = ByteBuffer.allocate(5);
      msg.put(msgType);
      msg.put(MathUtils.intToByteArray(uid));
      msg.clear();
    }

    @Override
    public String toString() {
      if (msgType == MSG_CANCEL) return "Cancel msg <"+uid+">";
      if (msgType == MSG_CANCEL_REQUEST) return "Cancel request <"+uid+">";
      return "Unknown message";
    }

    @Override
    public void complete() {
    }
    @Override
    public void drop() {
    }
    @Override
    public byte getPriority() {
      return CANCEL_PRIORITY;
    }
    @Override
    public long getSeq() {
      return Integer.MIN_VALUE;
    }
    @Override
    public int getUid() {
      return uid;
    }
    @Override
    public int compareTo(MessageWrapper that) {
      if (this.getPriority() == that.getPriority()) {
        if (this.uid == that.getUid()) {
          return Long.compare(this.getSeq(), that.getSeq());
        }
        return this.uid-that.getUid();
      }
      return this.getPriority()-that.getPriority();
    }

    /**
     * @return true if should keep writing
     */
    @Override
    public boolean receiveSelectResult(AppSocket socket) throws IOException {
      logger.trace("{}.receiveSelectResult({})", this, socket);

      long bytesWritten;
      if ((bytesWritten = socket.write(msg)) == -1) {
        // socket was closed, panic
        transfer.socketClosed();
        return false;
      }

      if (msg.hasRemaining()) {
        logger.trace("{}.rsr({}) has remaining", this, socket);
        return false;
      }
      return FileTransferSender.this.complete(this);
    }


  }

}
