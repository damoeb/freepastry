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
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CHUNK;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import rice.Continuation;
import rice.p2p.util.MathUtils;

/**
 * Each time complete is called, schedule the next one chunk.
 *
 * @author Jeff Hoye
 */
class OutgoingMessageReceipt extends OutgoingReceipt implements ByteBufferReceipt {
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

  public OutgoingMessageReceipt(FileTransferSender sender, ByteBuffer buffer, byte priority, int uid, Continuation<ByteBufferReceipt, Exception> continuation) {
    super(sender, priority, uid);
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

    outstanding = new MessageWrapperImpl(sender, this,wrapperSeq++,msgList);
    sender.enqueue(outstanding);
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
    sender.transfer.notifyListenersSendMsgProgress(this,msg.position()-initialPosition, msg.limit()-initialPosition);

    // if need to send more:
    if (msg.hasRemaining()) {
      // Construct a chunk (note that we reuse all the objects...)
      // Construct a BB of the right size
      if (msg.remaining() > sender.transfer.CHUNK_SIZE) {
        // don't send the whole message
        chunkBuffer.limit(msg.position()+sender.transfer.CHUNK_SIZE);
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
      sender.enqueue(outstanding);
      return;
    }

    // we've sent the whole message
    sender.outgoingData.remove(uid);
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
