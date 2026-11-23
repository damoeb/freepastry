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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.util.BoundedPriorityWriteQueue;
import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.environment.params.Parameters;
import rice.environment.processing.Processor;
import rice.environment.processing.WorkRequest;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.p2p.util.MathUtils;
import rice.selector.SelectorLoop;

/**
 * @author Jeff Hoye
 *
 */
public class FileTransferImpl implements FileTransfer, AppSocketReceiver {
  /**
   * Contains int UID, long offset, long length, int nameLength, UTF name
   */
  public static final byte MSG_FILE_HEADER = 1;

  /**
   * Contains int UID, int length
   */
  public static final byte MSG_BB_HEADER = 2;

  /**
   * Contains int UID, int chunk_length, byte[chunk_length] msg
   */
  public static final byte MSG_CHUNK = 3;

  /**
   * Contains int UID
   */
  public static final byte MSG_CANCEL = 4;

  /**
   * Contains int UID
   */
  public static final byte MSG_CANCEL_REQUEST = 5;


  FileAllocationStrategy fileAllocater;
  final AppSocket socket;
  final FileTransferCallback callback;

  int MAX_QUEUE_SIZE = 1024;

  /**
   * Max size of a message, (the size that can't be preempted)
   */
  int CHUNK_SIZE = 8192;

  int NUM_OUTSTANDING_CHUNKS_TO_WRITE_TO_NETWORK = 10;

  /** Parameter names and defaults of the bounds on what a remote peer may announce. */
  static final String PARAM_MAX_MESSAGE_SIZE = "p2p_filetransfer_max_message_size";
  static final String PARAM_MAX_FILE_SIZE = "p2p_filetransfer_max_file_size";
  static final String PARAM_MAX_NAME_SIZE = "p2p_filetransfer_max_name_size";
  static final String PARAM_MAX_CHUNK_SIZE = "p2p_filetransfer_max_chunk_size";
  static final String PARAM_MAX_MESSAGE_BYTES_IN_FLIGHT = "p2p_filetransfer_max_message_bytes_in_flight";
  static final int DEFAULT_MAX_MESSAGE_SIZE = 16 * 1024 * 1024;
  static final long DEFAULT_MAX_FILE_SIZE = 4L * 1024 * 1024 * 1024;
  static final int DEFAULT_MAX_NAME_SIZE = 4096;
  static final int DEFAULT_MAX_CHUNK_SIZE = 1024 * 1024;
  static final long DEFAULT_MAX_MESSAGE_BYTES_IN_FLIGHT = 64L * 1024 * 1024;

  final int maxMessageSize;
  final long maxFileSize;
  final int maxNameSize;
  final int maxChunkSize;
  final long maxMessageBytesInFlight;

  private final SelectorLoop selectorManager;
  final Logger logger;

  Processor processor;
  final Environment environment;

  public static final byte CANCEL_PRIORITY = -20;

  boolean failed = false;

  public static final int MAX_FILE_CHUNKS_IN_MEMORY = 100;

  /** The receiving half: reads the wire protocol and keeps the incoming transfers. */
  private final FileTransferReceiver receiver;

  public FileTransferImpl(AppSocket socket, FileTransferCallback callback, FileAllocationStrategy fileAllocater, Environment env, Processor processor) {
    this(socket,callback,fileAllocater,env,processor,-1,-1);
  }

  public FileTransferImpl(AppSocket socket, FileTransferCallback callback,
      FileAllocationStrategy fileAllocater, Environment env, Processor processor, int chunkSize, int numChunksToWriteToNetwork) {
    if (chunkSize > 0) {
      CHUNK_SIZE = chunkSize;
    }
    if (numChunksToWriteToNetwork > 0) {
      NUM_OUTSTANDING_CHUNKS_TO_WRITE_TO_NETWORK = numChunksToWriteToNetwork;
    }
    this.socket = socket;
    this.callback = callback;
    this.fileAllocater = fileAllocater;
    if (this.fileAllocater == null) this.fileAllocater = new TempFileAllocationStrategy();
    this.selectorManager = env.getSelectorManager();
    this.logger = env.getLogManager().getLogger(FileTransferImpl.class, null);
    this.queue = new BoundedPriorityWriteQueue<MessageWrapper>(MAX_QUEUE_SIZE, MessageWrapper::drop, logger);
    this.processor = processor;
    if (this.processor == null) this.processor = env.getProcessor();
    this.environment = env;
    Parameters params = env.getParameters();
    this.maxMessageSize = params.contains(PARAM_MAX_MESSAGE_SIZE) ? params.getInt(PARAM_MAX_MESSAGE_SIZE) : DEFAULT_MAX_MESSAGE_SIZE;
    this.maxFileSize = params.contains(PARAM_MAX_FILE_SIZE) ? params.getLong(PARAM_MAX_FILE_SIZE) : DEFAULT_MAX_FILE_SIZE;
    this.maxNameSize = params.contains(PARAM_MAX_NAME_SIZE) ? params.getInt(PARAM_MAX_NAME_SIZE) : DEFAULT_MAX_NAME_SIZE;
    // a chunk is at least as large as this node's own chunk size
    this.maxChunkSize = Math.max(CHUNK_SIZE, params.contains(PARAM_MAX_CHUNK_SIZE) ? params.getInt(PARAM_MAX_CHUNK_SIZE) : DEFAULT_MAX_CHUNK_SIZE);
    this.maxMessageBytesInFlight = params.contains(PARAM_MAX_MESSAGE_BYTES_IN_FLIGHT) ? params.getLong(PARAM_MAX_MESSAGE_BYTES_IN_FLIGHT) : DEFAULT_MAX_MESSAGE_BYTES_IN_FLIGHT;

    this.receiver = new FileTransferReceiver(this);

    socket.register(true, false, -1, this);
  }

  public FileTransferImpl(AppSocket socket, FileTransferCallback callback, Environment env) {
    this(socket, callback, null, env);
  }

  public FileTransferImpl(AppSocket socket, FileTransferCallback callback, FileAllocationStrategy fileAllocater, Environment env) {
    this(socket,callback,fileAllocater, env,null);
  }

  protected void socketClosed() {
    receiveException(socket,new ClosedChannelException("Underlying socket was closed."));
  }

  @Override
  public void receiveException(AppSocket socket, Exception e) {
    // warn user
    synchronized(queue) {
      if (failed) return;
    }
    if (callback != null) callback.receiveException(e);
    purge();
  }

  protected void purge() {
    Iterable<MessageWrapper> dropMe;
    synchronized(queue) {
      failed = true;
      dropMe = queue.waiting();
    }
    for (MessageWrapper foo : dropMe) {
      foo.drop();
    }
    receiver.failIncomingTransfers();
    for (ReceiptImpl receiptImpl : new ArrayList<ReceiptImpl>(outgoingData.values())) {
      receiptImpl.failed();
    }
  }

  int seq = Integer.MIN_VALUE;
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
  protected void scheduleToWriteIfNeeded() {
    if (!selectorManager.isSelectorThread()) throw new IllegalStateException("Must be called on the selector");

    // register on the writingSocket if needed
    if (!registered) {
      if (haveMessageToSend()) {
        // maybe we should remember if we were registered, and don't reregister, but for now it doesn't hurt
        registered = true;  // may fail in this call and set registered back to false, so make sure to do this before calling register
        logger.trace("{}.scheduleToWriteIfNeeded() registering to write", this);
        socket.register(false, true, 300000, this);
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
  protected boolean complete(MessageWrapper wrapper) {
    logger.trace("{}.complete({})", this, wrapper);
    if (wrapper != queue.inFlight()) throw new IllegalArgumentException("Wrapper:"+wrapper+" messageThatIsBeingWritten:"+queue.inFlight());

    queue.clearInFlight();

    // notify deliverAckToMe
    wrapper.complete();

    // close the socket if we need to
    return true;
  }

  final List<FileTransferListener> listeners = new CopyOnWriteArrayList<FileTransferListener>();
  @Override
  public void addListener(FileTransferListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(FileTransferListener listener) {
    listeners.remove(listener);
  }

  public Iterable<FileTransferListener> getListeners() {
    return listeners;
  }

  protected void notifyListenersSendMsgProgress(ByteBufferReceipt receipt,
      int bytesSent, int bytesTotal) {
    logger.debug("notifyListenersSendMsgProgress({},{},{})", receipt, bytesSent, bytesTotal);
    for (FileTransferListener fileTransferListener : getListeners()) {
      fileTransferListener.msgTransferred(receipt, bytesSent, bytesTotal, false);
    }
  }

  protected void notifyListenersReceiveMsgProgress(ByteBufferReceipt receipt,
      int bytesReceived, int bytesTotal) {
    logger.debug("notifyListenersReceiveMsgProgress({},{},{})", receipt, bytesReceived, bytesTotal);
    for (FileTransferListener fileTransferListener : getListeners()) {
      fileTransferListener.msgTransferred(receipt, bytesReceived, bytesTotal, true);
    }
  }

  protected void notifyListenersSendFileProgress(FileReceipt receipt,
      long bytesSent, long bytesTotal) {
    logger.debug("notifyListenersSendFileProgress({},{},{})", receipt, bytesSent, bytesTotal);
    for (FileTransferListener fileTransferListener : getListeners()) {
      fileTransferListener.fileTransferred(receipt, bytesSent, bytesTotal, false);
    }
  }

  protected void notifyListenersReceiveFileProgress(FileReceipt receipt,
      long bytesReceived, long bytesTotal) {
    logger.debug("notifyListenersReceiveFileProgress({},{},{})", receipt, bytesReceived, bytesTotal);
    for (FileTransferListener fileTransferListener : getListeners()) {
      fileTransferListener.fileTransferred(receipt, bytesReceived, bytesTotal, true);
    }
  }

  protected void notifyListenersSenderCancelled(Receipt receipt) {
    logger.debug("notifyListenersSenderCancelled({})", receipt);
    for (FileTransferListener fileTransferListener : getListeners()) {
      fileTransferListener.transferCancelled(receipt, true);
    }
  }

  protected void notifyListenersReceiverCancelled(Receipt receipt) {
    logger.debug("notifyListenersReceiverCancelled({})", receipt);
    for (FileTransferListener fileTransferListener : getListeners()) {
      fileTransferListener.transferCancelled(receipt, false);
    }
  }

  protected void notifyListenersTransferFailed(Receipt receipt, boolean incoming) {
    logger.debug("notifyListenersTransferFailed({})", receipt);
    for (FileTransferListener fileTransferListener : getListeners()) {
      fileTransferListener.transferFailed(receipt, incoming);
    }
  }

  /**
   * @return the length of the file, or 0 if it does not exist or cannot be read, as File.length() did
   */
  private static long length(Path path) {
    try {
      return Files.size(path);
    } catch (IOException e) {
      return 0;
    }
  }

  @Override
  public FileReceipt sendFile(Path path, ByteBuffer metadata, byte priority,
      Continuation<FileReceipt, Exception> continuation) throws IOException  {
    return sendFile(path,metadata,priority,0,length(path),continuation);
  }

  public FileReceipt sendFile(Path path, ByteBuffer metadataBB, byte priority, long offset, long length,
      Continuation<FileReceipt, Exception> continuation) throws IOException {
    if (path == null || !Files.exists(path) || Files.isDirectory(path)) throw new IllegalArgumentException("File f must be non-null, exist, and must not be a directory. "+path);
    byte[] metadata = new byte[metadataBB.remaining()];
    metadataBB.get(metadata);
    FileReceiptImpl ret = new FileReceiptImpl(path,metadata,priority,offset,length,allocateTransferUid(),continuation);
    return ret;
  }

  @Override
  public ByteBufferReceipt sendMessage(ByteBuffer buffer, byte priority,
      Continuation<ByteBufferReceipt, Exception> continuation) {
    if (buffer == null) throw new IllegalArgumentException("ByteBuffer bb must be non-null");
    BBReceiptImpl ret = new BBReceiptImpl(buffer,priority,allocateTransferUid(),continuation);
    return ret;
  }

  protected int allocateTransferUid() {
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
      notifyListenersTransferFailed(this, false);
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
      notifyListenersSendMsgProgress(this,msg.position()-initialPosition, msg.limit()-initialPosition);

      // if need to send more:
      if (msg.hasRemaining()) {
        // Construct a chunk (note that we reuse all the objects...)
        // Construct a BB of the right size
        if (msg.remaining() > CHUNK_SIZE) {
          // don't send the whole message
          chunkBuffer.limit(msg.position()+CHUNK_SIZE);
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
      for (int i = 0; i < NUM_OUTSTANDING_CHUNKS_TO_WRITE_TO_NETWORK-1; i++) {
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

        final int amtToRead = (int)Math.min(CHUNK_SIZE,lastByte-ptr);
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
            FileTransferImpl.this.sendCancel(uid);
          }

        };

        processor.processBlockingIO(new WorkRequest<Object>(c, environment.getSelectorManager()) {

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
      processor.processBlockingIO(new WorkRequest<Object>(c2, environment.getSelectorManager()) {
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
      notifyListenersSendFileProgress(this,ptr-initialPosition, length);

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

      processor.processBlockingIO(new WorkRequest<Object>(c,environment.getSelectorManager()) {

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
          socketClosed();
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
      return FileTransferImpl.this.complete(this);
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

  @Override
  public void receiveSelectResult(AppSocket socket, boolean canRead, boolean canWrite) {

    if (canWrite) {
      try {
        registered = false;
        logger.trace("receivedSelectResult({},{},{}", socket, canRead, canWrite);
        MessageWrapper current = queue.poll();
        while (current != null && current.receiveSelectResult(socket)) {
          current = queue.poll();
        }
        scheduleToWriteIfNeeded();
      } catch (IOException ioe) {
        // note, clearAndEnqueue() gets called later by the writer when the stack unravels again
        logger.trace("{}.rsr({})", this, socket, ioe);
        receiveException(socket, ioe);
        return;
      }
    }
    if (canRead) {
      try {
        // keep reading
        while(receiver.read(socket));
        // always be reading
        registerToReadIfPossible();
      } catch (IOException ioe) {
        failAndClose(ioe);
      } catch (RuntimeException e) {
        // whatever the peer sent, it must not escape as an unchecked exception on the selector thread
        failAndClose(new IOException("Protocol error: " + e, e));
      }
    }
  }

  public void registerToReadIfPossible() {
    receiver.registerToReadIfPossible();
  }

  /**
   * The peer broke the protocol or the socket failed: close the socket, then tell the user and drop everything. The
   * socket is closed first so that whoever is told of the failure never sees a socket that is still open.
   */
  private void failAndClose(IOException ioe) {
    try {
      socket.close();
    } finally {
      receiveException(socket, ioe);
    }
  }




  protected void receiverCancelled(int uid) {
    ReceiptImpl writer = outgoingData.remove(uid);
    if (writer != null) {
      writer.cancel();
      writer.notifyReceiverCancelled();
      notifyListenersReceiverCancelled(writer);
    } else {
      logger.warn("receiverCanclled({}) no record of the uid.", uid);
    }
  }



  @Override
  public void receiveSocket(AppSocket socket) {
    throw new RuntimeException("Not Implemented, shouldn't be called.");
  }

  /**
   * Only call this on the receiver side, b/c uid is simplex
   * @return if we can guarantee the request was cancelled
   */
  protected boolean requestCancel(int uid) {
    enqueue(new SimpleMessageWrapper(MSG_CANCEL_REQUEST, uid));
    return false;
  }

  /**
   * Only call this on the sender side, b/c uid is simplex
   */
  protected boolean sendCancel(int uid) {
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
        socketClosed();
        return false;
      }

      if (msg.hasRemaining()) {
        logger.trace("{}.rsr({}) has remaining", this, socket);
        return false;
      }
      return FileTransferImpl.this.complete(this);
    }


  }

}
