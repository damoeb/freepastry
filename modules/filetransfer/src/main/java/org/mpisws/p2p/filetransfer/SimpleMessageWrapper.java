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
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CANCEL;
import static org.mpisws.p2p.filetransfer.FileTransferImpl.MSG_CANCEL_REQUEST;

import java.io.IOException;
import java.nio.ByteBuffer;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.util.MathUtils;

/** The queue entry of a cancel or cancel request: five bytes, ahead of everything else. */
class SimpleMessageWrapper implements MessageWrapper {
  final FileTransferSender sender;
  ByteBuffer msg;
  int uid;
  byte msgType;
  public SimpleMessageWrapper(FileTransferSender sender, byte msgType, int uid) {
    this.sender = sender;
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
    sender.logger.trace("{}.receiveSelectResult({})", this, socket);

    long bytesWritten;
    if ((bytesWritten = socket.write(msg)) == -1) {
      // socket was closed, panic
      sender.transfer.socketClosed();
      return false;
    }

    if (msg.hasRemaining()) {
      sender.logger.trace("{}.rsr({}) has remaining", this, socket);
      return false;
    }
    return sender.complete(this);
  }


}
