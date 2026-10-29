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
package org.mpisws.p2p.transport.peerreview.replay.playback;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactoryImpl;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA1HashProvider;
import org.mpisws.p2p.transport.util.Serializer;
import org.slf4j.LoggerFactory;
import rice.environment.Environment;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Replays a SEND against the SEND entry in the log.
 */
public class ReplayVerifierSendTest implements PeerReviewConstants {

  static final Integer LOCAL = 1;
  static final Integer TARGET = 2;
  static final byte[] MESSAGE = {10, 11, 12, 13, 14, 15, 16, 17};

  static final Serializer<Integer> SERIALIZER = new Serializer<Integer>() {
    public void serialize(Integer i, OutputBuffer buf) throws IOException {
      buf.writeInt(i);
    }

    public Integer deserialize(InputBuffer buf) throws IOException {
      return buf.readInt();
    }
  };

  /** Only send() is exercised. */
  static class SendVerifier extends ReplayVerifier<Integer> {
    SendVerifier(HashProvider hashProv, SecureHistory history) throws IOException {
      super(SERIALIZER, hashProv, history, LOCAL, (short)0, hashProv.getHashSizeBytes(), 1,
          LoggerFactory.getLogger(SendVerifier.class));
    }

    protected void receive(Integer from, ByteBuffer msg) {
      throw new UnsupportedOperationException();
    }

    protected void incomingSocket(Integer from, int socketId) {
      throw new UnsupportedOperationException();
    }

    protected void socketIO(int socketId, boolean canRead, boolean canWrite) {
      throw new UnsupportedOperationException();
    }

    protected void socketOpened(int socketId) {
      throw new UnsupportedOperationException();
    }

    protected void socketException(int socketId, IOException ioe) {
      throw new UnsupportedOperationException();
    }

    public Environment getEnvironment() {
      return null;
    }

    public SocketRequestHandle<Integer> openSocket(Integer i, SocketCallback<Integer> deliverSocketToMe,
        Map<String, Object> options) {
      throw new UnsupportedOperationException();
    }

    public MessageRequestHandle<Integer, ByteBuffer> sendMessage(Integer i, ByteBuffer m,
        MessageCallback<Integer, ByteBuffer> deliverAckToMe, Map<String, Object> options) {
      throw new UnsupportedOperationException();
    }

    public Integer getLocalIdentifier() {
      return localHandle;
    }

    public void setCallback(TransportLayerCallback<Integer, ByteBuffer> callback) {
    }

    public void setErrorHandler(ErrorHandler<Integer> handler) {
    }

    public void destroy() {
    }

    public void acceptMessages(boolean b) {
    }

    public void acceptSockets(boolean b) {
    }
  }

  @TempDir
  File dir;

  Environment env;
  HashProvider hashProv;
  SecureHistory history;

  @BeforeEach
  public void setUp() throws Exception {
    env = new Environment();
    hashProv = new SHA1HashProvider();
    history = new SecureHistoryFactoryImpl(hashProv, env).create(new File(dir, "log").getPath(), 0, hashProv.getEmptyHash());
  }

  @AfterEach
  public void tearDown() throws IOException {
    history.close();
    env.destroy();
  }

  /** The SEND entry as it is logged: the receiver, then the message. */
  byte[] sendEntry(Integer target, byte[] message) throws IOException {
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    SERIALIZER.serialize(target, buf);
    buf.write(message, 0, message.length);
    return buf.getBytes();
  }

  /** The SEND entry of a partly relevant message: the receiver, the relevant part, then the hash of the rest. */
  byte[] partlyHashedSendEntry(Integer target, byte[] message, int relevantLen) throws IOException {
    byte[] irrelevantHash = hashProv.hash(ByteBuffer.wrap(message, relevantLen, message.length - relevantLen));
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    buf.write(sendEntry(target, Arrays.copyOf(message, relevantLen)));
    buf.write(irrelevantHash, 0, irrelevantHash.length);
    return buf.getBytes();
  }

  @Test
  public void hashedPartlyRelevantSendMatchesTheSameMessage() throws IOException {
    history.appendHash(EVT_SEND, hashProv.hash(ByteBuffer.wrap(partlyHashedSendEntry(TARGET, MESSAGE, 3))));
    SendVerifier verifier = new SendVerifier(hashProv, history);

    verifier.send(TARGET, ByteBuffer.wrap(MESSAGE), 3);

    assertTrue(verifier.verifiedOK());
  }

  @Test
  public void hashedPartlyRelevantSendDetectsADifferentIrrelevantPart() throws IOException {
    history.appendHash(EVT_SEND, hashProv.hash(ByteBuffer.wrap(partlyHashedSendEntry(TARGET, MESSAGE, 3))));
    SendVerifier verifier = new SendVerifier(hashProv, history);

    byte[] other = MESSAGE.clone();
    other[other.length - 1]++;
    verifier.send(TARGET, ByteBuffer.wrap(other), 3);

    assertFalse(verifier.verifiedOK(), "the irrelevant part is covered by its hash");
  }

  /** A logged (not hashed) SEND entry of a partly relevant message, with the flag that marks it as such. */
  byte[] flaggedPartlyHashedSendEntry(Integer target, byte[] message, int relevantLen) throws IOException {
    byte[] irrelevantHash = hashProv.hash(ByteBuffer.wrap(message, relevantLen, message.length - relevantLen));
    SimpleOutputBuffer buf = new SimpleOutputBuffer();
    SERIALIZER.serialize(target, buf);
    buf.writeBoolean(true);
    buf.write(message, 0, relevantLen);
    buf.write(irrelevantHash, 0, irrelevantHash.length);
    return buf.getBytes();
  }

  SendVerifier loggedPartlyRelevantSend() throws IOException {
    history.appendEntry(EVT_SEND, true, ByteBuffer.wrap(flaggedPartlyHashedSendEntry(TARGET, MESSAGE, 3)));
    SendVerifier verifier = new SendVerifier(hashProv, history);
    verifier.useLogHashFlag = true;
    return verifier;
  }

  @Test
  public void loggedPartlyRelevantSendMatchesTheSameMessage() throws IOException {
    SendVerifier verifier = loggedPartlyRelevantSend();

    verifier.send(TARGET, ByteBuffer.wrap(MESSAGE), 3);

    assertTrue(verifier.verifiedOK());
  }

  @Test
  public void loggedPartlyRelevantSendDetectsADifferentRelevantPart() throws IOException {
    SendVerifier verifier = loggedPartlyRelevantSend();

    byte[] other = MESSAGE.clone();
    other[0]++;
    verifier.send(TARGET, ByteBuffer.wrap(other), 3);

    assertFalse(verifier.verifiedOK());
  }

  @Test
  public void loggedPartlyRelevantSendDetectsADifferentIrrelevantPart() throws IOException {
    SendVerifier verifier = loggedPartlyRelevantSend();

    byte[] other = MESSAGE.clone();
    other[other.length - 1]++;
    verifier.send(TARGET, ByteBuffer.wrap(other), 3);

    assertFalse(verifier.verifiedOK());
  }

  @Test
  public void hashedSendMatchesTheSameMessage() throws IOException {
    history.appendHash(EVT_SEND, hashProv.hash(ByteBuffer.wrap(sendEntry(TARGET, MESSAGE))));
    SendVerifier verifier = new SendVerifier(hashProv, history);

    verifier.send(TARGET, ByteBuffer.wrap(MESSAGE), -1);

    assertTrue(verifier.verifiedOK(), "the hash of the replayed SEND equals the hash in the log");
  }

  @Test
  public void hashedSendDetectsADifferentMessage() throws IOException {
    history.appendHash(EVT_SEND, hashProv.hash(ByteBuffer.wrap(sendEntry(TARGET, MESSAGE))));
    SendVerifier verifier = new SendVerifier(hashProv, history);

    byte[] other = MESSAGE.clone();
    other[0]++;
    verifier.send(TARGET, ByteBuffer.wrap(other), -1);

    assertFalse(verifier.verifiedOK());
  }
}
