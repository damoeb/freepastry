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

import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CHUNK;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_FILE_HEADER;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import rice.Continuation;
import rice.Futures;
import rice.environment.processing.WorkRequest;
import rice.p2p.util.MathUtils;

/**
 * Keep up to FILE_CACHE of chunks scheduled, and every time one finishes, read some more to schedule more.
 *
 * TODO: Make more than 1 outstanding chunk
 *
 * @author Jeff Hoye
 */
class OutgoingFileReceipt extends OutgoingReceipt implements FileReceipt {
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


  public OutgoingFileReceipt(FileTransferSender sender, Path path, byte[] metadata, byte priority, long offset, long length, int uid, Continuation<FileReceipt, Exception> continuation) throws IOException {
    super(sender, priority, uid);
    if (offset+length > FileTransferSender.length(path)) throw new IllegalArgumentException("File is only "+FileTransferSender.length(path)+" but you are trying to send "+length+" bytes starting at "+offset);
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

    MessageWrapperImpl foo = new MessageWrapperImpl(sender, this,wrapperSeq++,msgList);
    sender.enqueue(foo);

    // NOTE: the last one comes when foo is written
    for (int i = 0; i < sender.transfer.NUM_OUTSTANDING_CHUNKS_TO_WRITE_TO_NETWORK-1; i++) {
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

      final int amtToRead = (int)Math.min(sender.transfer.CHUNK_SIZE,lastByte-ptr);
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
          sender.sendCancel(uid);
        }

      };

      Futures.forward(sender.transfer.processor.processBlockingIO(() -> {
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
        sender.enqueue(new MessageWrapperImpl(sender, OutgoingFileReceipt.this,seq,foo));
        return null;
      }, sender.transfer.environment.getSelectorManager()), c, sender.transfer.environment);

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
        sender.outgoingData.remove(uid);
        if (deliverAckToMe != null) deliverAckToMe.receiveResult(OutgoingFileReceipt.this);
      }

      @Override
      public void receiveException(Exception exception) {
        sender.logger.warn("Error closing file <{}> {} {}", uid, file, metadata.length, exception);
      }

    };
    Futures.forward(sender.transfer.processor.processBlockingIO(() -> {
      file.close();
      return null;
    }, sender.transfer.environment.getSelectorManager()), c2, sender.transfer.environment);
  }

  @Override
  void complete(MessageWrapper wrapper) {
    // notify listener
    sender.transfer.notifyListenersSendFileProgress(this,ptr-initialPosition, length);

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

        OutgoingFileReceipt.this.superCancel();
      }

      @Override
      public void receiveException(Exception exception) {

      }
    };

    // does nothing, just calls the above continuation after flushing out any existing stuff in the processor
    Futures.forward(sender.transfer.processor.processBlockingIO(() -> null, sender.transfer.environment.getSelectorManager()), c, sender.transfer.environment);
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
