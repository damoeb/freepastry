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
package org.mpisws.p2p.transport.peerreview.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtRecv;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.util.Serializer;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.rawserialization.WireLengths;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.CHAL_SEND;

/**
 *
 * Note, it's only Evidence when the type has been changed.
 *
  MSG_USERDATA
  byte type = MSG_USERDATA
  long long topSeq
  handle senderHandle
  hash hTopMinusOne
  signature sig
  byte relevantCode          // 0xFF = fully, otherwise length in bytes
  [payload bytes follow]

 * @author Jeff Hoye
 *
 */
public sealed class UserDataMessage<Handle extends RawSerializable> implements PeerReviewMessage, Evidence
    permits OutgoingUserDataMessage {
  /** Relevant-length code meaning the whole payload is relevant; shorter lengths are sent as the length itself. */
  public static final byte RELEVANT_LEN_ALL = (byte)0xFF;

  long topSeq;
  Handle senderHandle;
  byte[] hTopMinusOne;
  byte[] signature;
  int relevantLen;
  private byte[] payload;

  public UserDataMessage(long topSeq, Handle senderHandle, byte[] topMinusOne,
      byte[] sig, ByteBuffer message, int relevantLen) {
    this.topSeq = topSeq;
    this.senderHandle = senderHandle;
    hTopMinusOne = topMinusOne;
    this.signature = sig;
    this.relevantLen = relevantLen;

    assert((relevantLen == message.remaining()) || (relevantLen < MathUtils.uByteToInt(RELEVANT_LEN_ALL)));
    if (message.remaining() == message.array().length) {
      this.payload = message.array();
    } else {
      this.payload = new byte[message.remaining()];
      System.arraycopy(message.array(), message.arrayOffset()+message.position(), payload, 0, payload.length);
    }
  }

  @Override
  public String toString() {
    return "UDM:"+topSeq+","+senderHandle+","+java.util.HexFormat.of().formatHex(hTopMinusOne)+","+java.util.HexFormat.of().formatHex(signature)+","+relevantLen+","+payload.length;
  }

  @Override
  public short getType() {
    return PeerReviewMessageType.USERDATA.id();
  }

  public byte getRelevantCode() {
    byte relevantCode = (relevantLen == payload.length) ? RELEVANT_LEN_ALL : (byte)relevantLen;
    return relevantCode;
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeLong(topSeq);
    senderHandle.serialize(buf);
    buf.write(hTopMinusOne, 0, hTopMinusOne.length);
    buf.write(signature, 0, signature.length);
    byte relevantCode = getRelevantCode();
    buf.writeByte(relevantCode);
      buf.write(payload, 0, payload.length);
  }

  public static <H extends RawSerializable> UserDataMessage<H> build(
      InputBuffer buf, Serializer<H> serializer, int hashSize, int sigSize) throws IOException {
    long seq = buf.readLong();
    H handle = serializer.deserialize(buf);
    byte[] hash = WireLengths.readBytes(buf, hashSize);
    byte[] sig = WireLengths.readBytes(buf, sigSize);
    byte relevantCode = buf.readByte();
    int len = MathUtils.uByteToInt(relevantCode);

    byte[] msg = WireLengths.readBytes(buf, buf.bytesRemaining());

    if (len == MathUtils.uByteToInt(RELEVANT_LEN_ALL)) {
      len = msg.length;
    }

    return new UserDataMessage<H>(seq, handle, hash, sig, ByteBuffer.wrap(msg), len);
  }

  public long getTopSeq() {
    return topSeq;
  }

  public Handle getSenderHandle() {
    return senderHandle;
  }

  public byte[] getPreviousTopHash() {
    return hTopMinusOne;
  }

  public byte[] getSignature() {
    return signature;
  }

  public int getRelevantLength() {
    return relevantLen;
  }

  public ByteBuffer getPayload() {
    return ByteBuffer.wrap(payload);
  }

  public EvtRecv<Handle> getReceiveEvent(HashProvider hasher) {
    if (getRelevantLength() < getPayload().remaining()) {
      return new EvtRecv<Handle>(getSenderHandle(), getTopSeq(), getPayload(), getRelevantLength(), hasher);
    } else {
      return new EvtRecv<Handle>(getSenderHandle(), getTopSeq(), getPayload());
    }
  }

  public byte[] getInnerHash(RawSerializable myId, HashProvider hasher) {
    try {
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      myId.serialize(sob);
      sob.writeBoolean(getRelevantLength() < getPayloadLength());
      return getInnerHash(sob.getByteBuffer(), hasher);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public byte[] getInnerHash(HashProvider hasher) {
    try {
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      senderHandle.serialize(sob);
      sob.writeLong(topSeq);
      sob.writeBoolean(relevantLen < getPayloadLength());
      return getInnerHash(sob.getByteBuffer(),hasher);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public byte[] getInnerHash(ByteBuffer header, HashProvider hasher) {
    /* The peer will have logged a RECV entry, and the signature is calculated over that
    entry. To verify the signature, we must reconstruct that RECV entry locally */

    if (relevantLen < payload.length) {
      byte[] irrelevantHash = hasher.hash(ByteBuffer.wrap(payload, relevantLen, payload.length-relevantLen));
      return hasher.hash(header, ByteBuffer.wrap(payload, 0, relevantLen), ByteBuffer.wrap(irrelevantHash));
    } else {
      return hasher.hash(header, ByteBuffer.wrap(payload));
    }

  }

  public int getPayloadLength() {
    return payload.length;
  }

  @Override
  public short getEvidenceType() {
    return CHAL_SEND;
  }
}
