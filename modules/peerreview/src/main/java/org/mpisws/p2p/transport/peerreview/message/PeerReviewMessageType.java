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

import rice.p2p.commonapi.rawserialization.MessageTypeId;
import rice.p2p.commonapi.rawserialization.MessageTypeIndex;

/**
 * The type ids of PeerReview's messages, written as one byte after
 * {@link org.mpisws.p2p.transport.peerreview.PeerReview#PEER_REVIEW_COMMIT} and read by
 * {@link org.mpisws.p2p.transport.peerreview.PeerReviewImpl}. They replace the former
 * {@code PeerReviewConstants.MSG_*} constants and keep their values.
 */
public enum PeerReviewMessageType implements MessageTypeId {
  /** Contains data the application has sent: {@link UserDataMessage} */
  USERDATA(16),
  /** Acknowledges a USERDATA message: {@link AckMessage} */
  ACK(17),
  /** Contains evidence about a third node: {@link AccusationMessage} */
  ACCUSATION(18),
  /** Contains evidence about the recipient: {@link ChallengeMessage} */
  CHALLENGE(19),
  /** Answers a previous CHALLENGE: {@link ResponseMessage} */
  RESPONSE(20),
  /** Sent to a witness; contains authenticators: {@link AuthPushMessage} */
  AUTHPUSH(21),
  /** Asks a witness to return a recent authenticator: {@link AuthRequest} */
  AUTHREQ(22),
  /** Responds to a previous AUTHREQ: {@link AuthResponse} */
  AUTHRESP(23),
  /** Contains a datagram from the app (not logged); reserved, no message class uses it */
  USERDGRAM(24);

  private static final MessageTypeIndex<PeerReviewMessageType> INDEX = MessageTypeIndex.of(values());

  private final short id;

  PeerReviewMessageType(int id) {
    this.id = (short) id;
  }

  @Override
  public short id() {
    return id;
  }

  /**
   * @param id a type id
   * @return the type with that id, or {@code null} if no PeerReview message has it
   */
  public static PeerReviewMessageType find(short id) {
    return INDEX.find(id);
  }
}
