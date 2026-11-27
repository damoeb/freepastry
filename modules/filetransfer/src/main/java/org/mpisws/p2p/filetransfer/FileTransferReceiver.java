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

import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_BB_HEADER;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CANCEL;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CANCEL_REQUEST;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CHUNK;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_FILE_HEADER;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import rice.Continuation;
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.util.PrivateFiles;
import rice.environment.processing.Processor;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.util.MathUtils;
import rice.selector.TimerTask;

/**
 * The receiving half of a {@link FileTransferImpl}: reads the wire protocol off the socket, one frame at a time, and
 * keeps the transfers that are being received (messages in memory, files on disk) until they are complete or cancelled.
 *
 * Runs on the selector thread, like the rest of the file transfer.
 */
class FileTransferReceiver {
  private final FileTransferImpl transfer;
  private final Logger logger;
  private final Environment environment;
  private final Processor processor;
  private final FileTransferCallback callback;
  private final FileAllocationStrategy fileAllocater;
  private final FileTransferSender sender;

  /**
   * Synchronized by only being mutated/checked on selector thread
   * can be reading when below MAX_PENDING_CHUNKS, allowed to be greater
   */
  private int fileChunksInMemory = 0;

  /** when the socket last had something to read, on the environment's clock */
  private long lastReadMillis;
  /** closes the socket when an incoming transfer stalls; scheduled while transfers are in progress */
  private TimerTask readWatchdog;

  FileTransferReceiver(FileTransferImpl transfer, FileTransferSender sender) {
    this.transfer = transfer;
    this.sender = sender;
    this.logger = transfer.logger;
    this.environment = transfer.environment;
    this.processor = transfer.processor;
    this.callback = transfer.callback;
    this.fileAllocater = transfer.fileAllocater;
  }

  /**
   * @return true if should keep reading
   */
  boolean read(AppSocket socket) throws IOException {
    return reader.read(socket);
  }

  /** Tells the listeners that every transfer that is still being received failed. */
  void failIncomingTransfers() {
    for (DataReader dataReader : new ArrayList<DataReader>(incomingData.values())) {
      transfer.notifyListenersTransferFailed(dataReader, true);
    }
  }

  /** The socket had something to read: an incoming transfer makes progress. */
  void noteRead() {
    lastReadMillis = environment.getTimeSource().currentTimeMillis();
  }

  /**
   * Starts the read timeout for the transfers now in progress: if no byte arrives for
   * {@link FileTransferImpl#readTimeoutMillis} while one is in progress and this side is reading, the socket is closed
   * and the transfers fail (design 3e).
   */
  private void watchReads() {
    if (transfer.readTimeoutMillis <= 0 || readWatchdog != null) return;
    noteRead();
    scheduleWatchdog(transfer.readTimeoutMillis);
  }

  private void scheduleWatchdog(long delayMillis) {
    readWatchdog = environment.getSelectorManager().schedule(new TimerTask() {
      @Override
      public void run() {
        readWatchdog = null;
        if (incomingData.isEmpty() || transfer.socket == null) return;
        long now = environment.getTimeSource().currentTimeMillis();
        if (fileChunksInMemory >= FileTransferImpl.MAX_FILE_CHUNKS_IN_MEMORY) {
          // this side stopped reading itself (chunks wait for the disk): the peer is not stalling
          lastReadMillis = now;
        }
        long idleMillis = now - lastReadMillis;
        if (idleMillis >= transfer.readTimeoutMillis) {
          transfer.failAndClose(new SocketTimeoutException("Nothing arrived for " + idleMillis + " ms while " + incomingData.size() + " incoming transfers were in progress"));
          return;
        }
        scheduleWatchdog(transfer.readTimeoutMillis - idleMillis);
      }
    }, delayMillis);
  }

  void incrementFileChunksInMemory() {
    if (!environment.getSelectorManager().isSelectorThread()) throw new IllegalStateException("Must be called on selector thread to maintain sync.");
    fileChunksInMemory++;
  }
  void decrementFileChunksInMemory() {
    if (!environment.getSelectorManager().isSelectorThread()) throw new IllegalStateException("Must be called on selector thread to maintain sync.");
    fileChunksInMemory--;
    registerToReadIfPossible();
  }

  void registerToReadIfPossible() {
    if (!environment.getSelectorManager().isSelectorThread()) throw new IllegalStateException("Must be called on selector thread to maintain sync.");
    if (fileChunksInMemory < FileTransferImpl.MAX_FILE_CHUNKS_IN_MEMORY) {
      transfer.socket.register(true, false, -1, transfer);
    } else {
    }
  }

  final MsgTypeReader msgTypeReader = new MsgTypeReader();
  final BBHeaderReader bbHeaderReader = new BBHeaderReader();
  final FileHeaderReader fileHeaderReader = new FileHeaderReader();
  final FileNameReader fileNameReader = new FileNameReader();
  final ChunkReader chunkReader = new ChunkReader();

  Reader reader = msgTypeReader;

  // Reading.
  interface Reader {

    /**
     * @return true if should keep reading
     */
    boolean read(AppSocket socket) throws IOException;
  }

  class MsgTypeReader implements Reader {
    byte[] bytes = new byte[5];
    ByteBuffer buf = ByteBuffer.wrap(bytes);

    @Override
    public boolean read(AppSocket socket) throws IOException {
      // read the
      long bytesRead = socket.read(buf);
      if (bytesRead < 0) {
        transfer.socketClosed();
        return false;
      }
      if (buf.hasRemaining()) return false;


      buf.clear();
      byte msgType = bytes[0];
      int uid = MathUtils.byteArrayToInt(bytes,1);
      buf.clear();

      switch(msgType) {
      case MSG_BB_HEADER:
        bbHeaderReader.setUID(uid);
        reader = bbHeaderReader;
        break;
      case MSG_FILE_HEADER:
        fileHeaderReader.setUID(uid);
        reader = fileHeaderReader;
        break;
      case MSG_CHUNK:
        chunkReader.setUID(uid);
        reader = chunkReader;
        break;
      case MSG_CANCEL:
        senderCancelled(uid);
        break;
      case MSG_CANCEL_REQUEST:
        sender.receiverCancelled(uid);
        break;
      }
      return true;
    }
  }

  void senderCancelled(int uid) {
    DataReader reader = incomingData.remove(uid);
    if (reader != null) {
      reader.cancelled(reader);
    } else {
      logger.warn("senderCanclled({}) no record of the uid.", uid);
    }
  }

  class BBHeaderReader implements Reader {
    byte[] bytes = new byte[4];
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    int uid;

    public void setUID(int uid) {
      this.uid = uid;
    }

    @Override
    public boolean read(AppSocket socket) throws IOException {
      // read the
      long bytesRead = socket.read(buf);
      if (bytesRead < 0) {
        transfer.socketClosed();
        return false;
      }
      if (buf.hasRemaining()) return false;

      buf.clear();
      int size = MathUtils.byteArrayToInt(bytes);
      buf.clear();

      addIncomingMessage(uid,size);

      reader = msgTypeReader;
      return true;
    }
  }

  class FileHeaderReader implements Reader {
    byte[] bytes = new byte[20];
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    int uid;

    public void setUID(int uid) {
      this.uid = uid;
    }

    @Override
    public boolean read(AppSocket socket) throws IOException {
      // read the
      long bytesRead = socket.read(buf);
      if (bytesRead < 0) {
        transfer.socketClosed();
        return false;
      }
      if (buf.hasRemaining()) return false;

      buf.clear();
      long offset = MathUtils.byteArrayToLong(bytes,0);
      long length = MathUtils.byteArrayToLong(bytes,8);
      int nameSize = MathUtils.byteArrayToInt(bytes,16);
      buf.clear();

      if (length < 0 || length > transfer.maxFileSize) throw new IOException("Protocol error: file " + uid + " announces " + length + " bytes, at most " + transfer.maxFileSize + " are accepted");
      if (offset < 0 || offset > length) throw new IOException("Protocol error: file " + uid + " announces offset " + offset + " in a file of " + length + " bytes");
      if (nameSize < 0 || nameSize > transfer.maxNameSize) throw new IOException("Protocol error: file " + uid + " announces a name of " + nameSize + " bytes, at most " + transfer.maxNameSize + " are accepted");
      if (incomingData.containsKey(uid)) throw new IOException("Protocol error: file " + uid + " is announced twice");

      fileNameReader.initialize(uid,offset,length,nameSize);
      reader = fileNameReader;
      return true;
    }
  }

  class FileNameReader implements Reader {
    byte[] bytes = new byte[20];
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    int uid;
    long offset;
    long length;

    public void initialize(int uid, long offset, long length, int nameSize) {
      this.uid = uid;
      this.offset = offset;
      this.length = length;
      if (bytes.length<nameSize) {
        bytes = new byte[nameSize];
        buf = ByteBuffer.wrap(bytes);
      }
      buf.clear();
      buf.limit(nameSize);
    }

    @Override
    public boolean read(AppSocket socket) throws IOException {
      // read the
      long bytesRead = socket.read(buf);
      if (bytesRead < 0) {
        transfer.socketClosed();
        return false;
      }
      if (buf.hasRemaining()) return false;

      buf.flip();
      byte[] returnBytes = new byte[buf.remaining()];
      buf.get(returnBytes);
      buf.clear();

      addIncomingFile(uid,returnBytes,offset,length);

      reader = msgTypeReader;
      return true;
    }
  }


  class ChunkReader implements Reader {
    byte[] bytes = new byte[4];
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    int uid;

    public void setUID(int uid) {
      this.uid = uid;
    }

    @Override
    public boolean read(AppSocket socket) throws IOException {
      // read the
      long bytesRead = socket.read(buf);
      if (bytesRead < 0) {
        transfer.socketClosed();
        return false;
      }
      if (buf.hasRemaining()) return false;

      buf.clear();
      int size = MathUtils.byteArrayToInt(bytes);
      buf.clear();

      DataReader dataReader = incomingData.get(uid);
      if (dataReader ==  null) throw new IOException("Protocol error: chunk for unknown uid "+uid);
      return dataReader.read(socket, size);
    }
  }

  private final Map<Integer, DataReader> incomingData = new HashMap<Integer, DataReader>();
  void addIncomingMessage(int uid, int size) throws IOException {
    if (incomingData.containsKey(uid)) throw new IOException("Protocol error: message "+uid+" is announced twice");
    if (size < 0 || size > transfer.maxMessageSize) throw new IOException("Protocol error: message " + uid + " announces " + size + " bytes, at most " + transfer.maxMessageSize + " are accepted");
    long inFlight = size;
    for (DataReader dataReader : incomingData.values()) {
      if (dataReader instanceof BBDataReader bBDataReader) inFlight += bBDataReader.bytes.length;
    }
    if (inFlight > transfer.maxMessageBytesInFlight) throw new IOException("Protocol error: message " + uid + " would bring the incoming messages to " + inFlight + " bytes, at most " + transfer.maxMessageBytesInFlight + " are accepted");
    BBDataReader bbdr = new BBDataReader(uid, size);
    incomingData.put(uid,bbdr);
    watchReads();
    transfer.notifyListenersReceiveMsgProgress(bbdr, 0, size);
    // the sender writes a chunk only for a message that has bytes, so nothing else would complete an empty one
    if (size == 0) {
      bbdr.complete();
    }
  }

  void addIncomingFile(int uid, byte[] metadata, long offset, long length) throws IOException {
    // check before the allocater creates a temp file
    if (incomingData.containsKey(uid)) throw new IOException("Protocol error: file "+uid+" is announced twice");
    long incomingFiles = incomingData.values().stream().filter(FileDataReader.class::isInstance).count();
    if (incomingFiles >= transfer.maxIncomingFiles) throw new IOException("Protocol error: file " + uid + " would be incoming file " + (incomingFiles + 1) + ", at most " + transfer.maxIncomingFiles + " are accepted at once");
    Path path = fileAllocater.getFile(ByteBuffer.wrap(metadata), offset, length);
    FileDataReader fdr = new FileDataReader(uid, metadata, path, offset, length);
    incomingData.put(uid,fdr);
    watchReads();
    transfer.notifyListenersReceiveFileProgress(fdr, 0, length);
    // the sender writes a chunk only for a file that has bytes, so nothing else would complete an empty one
    if (length == 0) {
      fdr.complete();
    }
  }

  interface DataReader extends Reader, Receipt {
    public boolean read(AppSocket socket, int size) throws IOException;
    public void cancelled(DataReader reader);
  }

  class BBDataReader implements DataReader, ByteBufferReceipt {
    int uid;
    byte[] bytes;
    ByteBuffer curReader;
    boolean requestedCancel = false;


    public BBDataReader(int uid, int size) {
      this.uid = uid;
      bytes = new byte[size];
      curReader = ByteBuffer.wrap(bytes);
      curReader.limit(0);
    }

    @Override
    public String toString() {
      return "Incoming msg<"+uid+"> of length"+bytes.length;
    }

    @Override
    public boolean read(AppSocket socket, int numToRead) throws IOException {
      if (curReader.hasRemaining()) throw new IllegalStateException("curReader has "+curReader.remaining()+" bytes remaining. "+numToRead);
      if (numToRead < 0 || numToRead > bytes.length - curReader.position()) {
        throw new IOException("Protocol error: chunk of " + numToRead + " bytes for a message with " + (bytes.length - curReader.position()) + " bytes to come");
      }
      curReader.limit(curReader.position()+numToRead);
      reader = this;
      return read(socket);
    }

    @Override
    public boolean read(AppSocket socket) throws IOException {
      long ret = socket.read(curReader);
      if (ret < 0) {
        transfer.socketClosed();
        return false;
      }
      if (curReader.hasRemaining()) {
        return false;
      }
      completeChunk();
      return true;
    }

    public void completeChunk() {
      // notify listeners
      transfer.notifyListenersReceiveMsgProgress(this, curReader.position(), bytes.length);

      if (curReader.position() == bytes.length) {
        complete();
      }
      reader = msgTypeReader;
    }

    public void complete() {
      incomingData.remove(uid);

      // notify callback
      callback.messageReceived(ByteBuffer.wrap(getBytes()));
    }

    @Override
    public byte[] getBytes() {
      return bytes;
    }

    @Override
    public byte getPriority() {
      throw new RuntimeException("Unknown priority.  Don't call this on the receiving side.");
    }

    @Override
    public long getSize() {
      return bytes.length;
    }

    @Override
    public int getUid() {
      return uid;
    }

    @Override
    public boolean cancel() {
      if (requestedCancel) return false;
      requestedCancel = true;
      return sender.requestCancel(uid);
    }

    // called when actually cancelled
    @Override
    public void cancelled(DataReader reader) {
      transfer.notifyListenersSenderCancelled(reader);
    }
  }

  class FileDataReader implements DataReader, FileReceipt {
    int uid;
    byte[] bytes;
    ByteBuffer curReader;
    RandomAccessFile file;

    Path f;
    byte[] metadata;
    long offset;
    long length;

    long ptr;
    /** Bytes of chunks accepted so far, only touched on the selector thread. */
    long received;
    private final Object ptrLock = new Object();

    boolean requestedCancel = false;
    boolean cancelled = false;

    /**
     * Null if there is no problem, set if there is a problem.
     */
    Exception exception = null;

    public FileDataReader(int uid, byte[] metadata, Path path, long offset, long length) throws IOException {
      this.uid = uid;
      this.f = path;
      this.ptr = offset;
      this.offset = offset;
      this.length = length;
      PrivateFiles.createIfAbsent(path);
      file = new RandomAccessFile(path.toFile(), "rw");
      this.metadata = metadata;
      file.seek(offset);
      bytes = new byte[transfer.CHUNK_SIZE];
      curReader = ByteBuffer.wrap(bytes);
    }

    @Override
    public String toString() {
      return "Incoming file<"+uid+"> "+metadata.length+" off:"+offset+" length:"+length+" "+f;
    }

    @Override
    public boolean read(AppSocket socket, int numToRead) throws IOException {
      if (curReader.position() != 0) throw new IllegalStateException("curReader has "+curReader.remaining()+" bytes remaining. "+numToRead);
      if (numToRead < 0 || numToRead > length || numToRead > transfer.maxChunkSize) {
        throw new IOException("Chunk of " + numToRead + " bytes in a file of " + length + " bytes, chunks of at most " + transfer.maxChunkSize + " are accepted");
      }
      if (received + numToRead > length) {
        throw new IOException("Chunk of " + numToRead + " bytes after " + received + " bytes of a file of " + length + " bytes");
      }
      received += numToRead;
      if (numToRead > bytes.length) {
        // the sender's chunk size is its own setting, the chunk is as large as it says
        bytes = new byte[numToRead];
        curReader = ByteBuffer.wrap(bytes);
      }
      curReader.limit(numToRead);
      reader = this;
      return read(socket);
    }

    @Override
    public boolean read(AppSocket socket) throws IOException {
      long ret = socket.read(curReader);
      if (ret < 0) {
        transfer.socketClosed();
        return false;
      }
      if (curReader.hasRemaining()) {
        return false;
      }
      completeChunk();

      // if there are too many workRequests, turn off read interest (don't schedule another reader)
      return true;
    }

    public void completeChunk() {
      curReader.flip();

      // copy the bytes so we can reuse them
      final byte[] writeMe = new byte[curReader.remaining()];
      curReader.get(writeMe);
      curReader.clear();
      incrementFileChunksInMemory();
      // schedule them to be written, then notified on the blockingIOThread

      // note, that it is required that these are in order
      Continuation<Long, Exception> written = new Continuation<Long, Exception>() {

        @Override
        public void receiveResult(Long myPtrL) {
          decrementFileChunksInMemory();
          if (cancelled) return;
          // notify listeners
          long myPtr = myPtrL.longValue();
          transfer.notifyListenersReceiveFileProgress(FileDataReader.this, myPtr-offset, length);
          if (myPtr == offset+length) FileDataReader.this.complete();
        }

        @Override
        public void receiveException(Exception exception) {
          logger.warn("Error writing file {} {}", f, metadata.length, exception);
          FileDataReader.this.cancel();
          decrementFileChunksInMemory();
        }
      };
      Futures.forward(processor.processBlockingIO(() -> {
        if (cancelled) return -1L;
        file.write(writeMe);
        synchronized(ptrLock) {
          ptr+=writeMe.length;
        }
        return ptr;
      }, environment.getSelectorManager()), written, environment);
      reader = msgTypeReader;
    }

    public void complete() {
      incomingData.remove(uid);
      try {
        file.close();
      } catch (IOException ioe) {
        logger.warn("Error closing file {}", file, ioe);
      }

      // notify callback
      callback.fileReceived(getFile(), getMetadata());
    }

    public byte[] getBytes() {
      return bytes;
    }

    @Override
    public byte getPriority() {
      throw new RuntimeException("Unknown priority.  Don't call this on the receiving side.");
    }

    @Override
    public long getSize() {
      return bytes.length;
    }

    @Override
    public int getUid() {
      return uid;
    }

    @Override
    public boolean cancel() {
      if (requestedCancel) return false;
      requestedCancel = true;
      return sender.requestCancel(uid);
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
      return offset;
    }

    // called when actually cancelled
    @Override
    public void cancelled(final DataReader reader) {
      cancelled = true;

      // Don't close the file on the wrong thread.
      Continuation<RandomAccessFile, Exception> closed = new Continuation<RandomAccessFile, Exception>() {

        @Override
        public void receiveResult(RandomAccessFile result) {
          logger.atInfo().log(() -> "File Cancelled<"+uid+"> "+f+","+offset+","+(ptr-offset)+","+length);
          fileAllocater.fileCancelled(ByteBuffer.wrap(metadata), f, offset, ptr-offset, length, exception);
          transfer.notifyListenersSenderCancelled(reader);
        }

        @Override
        public void receiveException(Exception exception) {
          logger.warn("Error closing file {}", file, exception);
        }
      };
      Futures.forward(processor.processBlockingIO(() -> {
        file.close();
        return file;
      }, environment.getSelectorManager()), closed, environment);

    }
  }

}
