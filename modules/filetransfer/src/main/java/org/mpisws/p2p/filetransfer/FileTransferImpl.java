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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.environment.params.Parameters;
import rice.environment.processing.Processor;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;

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

  final Logger logger;

  Processor processor;
  final Environment environment;

  public static final byte CANCEL_PRIORITY = -20;

  public static final int MAX_FILE_CHUNKS_IN_MEMORY = 100;

  /** The receiving half: reads the wire protocol and keeps the incoming transfers. */
  private final FileTransferSender sender;
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
    this.logger = env.getLogManager().getLogger(FileTransferImpl.class, null);
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

    this.sender = new FileTransferSender(this);
    this.receiver = new FileTransferReceiver(this, sender);

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
    if (sender.hasFailed()) return;
    if (callback != null) callback.receiveException(e);
    purge();
  }

  protected void purge() {
    sender.dropQueuedMessages();
    receiver.failIncomingTransfers();
    sender.failOutgoingTransfers();
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

  @Override
  public FileReceipt sendFile(Path path, ByteBuffer metadata, byte priority,
      Continuation<FileReceipt, Exception> continuation) throws IOException  {
    return sendFile(path,metadata,priority,0,FileTransferSender.length(path),continuation);
  }

  @Override
  public void receiveSelectResult(AppSocket socket, boolean canRead, boolean canWrite) {

    if (canWrite) {
      try {
        logger.trace("receivedSelectResult({},{},{}", socket, canRead, canWrite);
        sender.writeQueued(socket);
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




  public FileReceipt sendFile(Path path, ByteBuffer metadataBB, byte priority, long offset, long length,
      Continuation<FileReceipt, Exception> continuation) throws IOException {
    return sender.sendFile(path, metadataBB, priority, offset, length, continuation);
  }

  @Override
  public ByteBufferReceipt sendMessage(ByteBuffer buffer, byte priority,
      Continuation<ByteBufferReceipt, Exception> continuation) {
    return sender.sendMessage(buffer, priority, continuation);
  }

  @Override
  public void receiveSocket(AppSocket socket) {
    throw new RuntimeException("Not Implemented, shouldn't be called.");
  }

}
