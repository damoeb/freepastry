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
import java.util.List;
import rice.p2p.commonapi.appsocket.AppSocket;

/** The queue entry of one chunk (or the header) of an {@link OutgoingReceipt}. */
class MessageWrapperImpl implements MessageWrapper {
  final FileTransferSender sender;
  boolean started = false;
  OutgoingReceipt receipt;
  List<ByteBuffer> message;
  long seq;  // the sequence inside of the OutgoingReceipt

  public MessageWrapperImpl(FileTransferSender sender, OutgoingReceipt receipt, long seq, List<ByteBuffer> message) {
    this.sender = sender;
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
    if (this.equals(sender.queue.inFlight())) {
      if (!started) {
        // nothing was written yet, so the message can still be cancelled: its receipt is cancelled too, and when the
        // socket calls back, receiveSelectResult() sees that and drops the message without writing it
        return true;
      } else {
        return false;
      }
    }
    return sender.queue.remove(this);
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
    sender.logger.trace("{}.receiveSelectResult({})", this, socket);

    if (receipt.isCancelled() && !started) {
      sender.logger.trace("{}.rsr({}) cancelled", this, socket);
      sender.queue.clearInFlight();
      return true;
    } else {
      started = true;
      long bytesWritten;
      if ((bytesWritten = socket.write(message.get(0))) == -1) {
        // socket was closed, panic
        sender.transfer.socketClosed();
        return false;
      }

      if (message.get(0).hasRemaining()) {
        sender.logger.trace("{}.rsr({}) has remaining", this, socket);
        return false;
      } else {
        // write the next BB
        message.remove(0);
        if (!message.isEmpty())
          return receiveSelectResult(socket);
      }
    }

    // notify that this MsgWrapper is complete
    return sender.complete(this);
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
