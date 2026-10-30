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
package org.mpisws.p2p.testing.transportlayer.peerreview;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.SortedSet;

import org.junit.jupiter.api.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.peerreview.IdentifierExtractor;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.audit.EvidenceTool;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializerImpl;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStore;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStoreImpl;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.identity.UnknownCertificateException;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.message.PeerReviewMessage;
import org.mpisws.p2p.transport.peerreview.replay.VerifierFactory;
import org.mpisws.p2p.transport.util.Serializer;
import rice.Continuation;
import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.p2p.commonapi.Cancellable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SIGNATURE_BAD;


public class AuthenticatorStoreTest {
  public static final int HASH_LEN = 20;
  public static final int SIGN_LEN = 28;



  @Test
  public void addAndFlushAuthenticators() throws Exception {
    Environment env = new Environment();
    try {
    PeerReview<MyInetSocketAddress, MyInetSocketAddress> pr = new TestPeerReview(env, new AuthenticatorSerializerImpl(HASH_LEN, SIGN_LEN));
    TestAuthenticatorStore store = new TestAuthenticatorStore(pr,false);

    MyInetSocketAddress id = new MyInetSocketAddress(InetAddress.getLocalHost(), 6789);

    byte[] h1 = new byte[HASH_LEN];
    h1[2] = 5;
    byte[] s1 = new byte[SIGN_LEN];
    s1[2] = 17;
    Authenticator a1 = new Authenticator(42,h1,s1);

    // same
    byte[] h2 = new byte[HASH_LEN];
    h2[2] = 5;
    byte[] s2 = new byte[SIGN_LEN];
    s2[2] = 17;
    Authenticator a2 = new Authenticator(42,h2,s2);

    // dif hash
    byte[] h3 = new byte[HASH_LEN];
    h3[2] = 8;
    byte[] s3 = new byte[SIGN_LEN];
    s3[2] = 17;
    Authenticator a3 = new Authenticator(42,h3,s3);

    store.addAuthenticatorToMemory(id, new Authenticator(4,h1,s1));
    store.addAuthenticatorToMemory(id, new Authenticator(7,h1,s1));
    store.addAuthenticatorToMemory(id, new Authenticator(8,h1,s1));
    store.addAuthenticatorToMemory(id, new Authenticator(9,h1,s1));
    store.addAuthenticatorToMemory(id, new Authenticator(41,h1,s1));
    store.addAuthenticatorToMemory(id, new Authenticator(43,h1,s1));
    store.addAuthenticatorToMemory(id, new Authenticator(53,h1,s1));

    store.addAuthenticatorToMemory(id, a1);
    store.addAuthenticatorToMemory(id, a2); // should not crash here

    // a different authenticator with the same sequence number is rejected
    assertThrows(RuntimeException.class, () -> store.addAuthenticatorToMemory(id, a3), "Allowed duplicates");

    store.flushAuthenticatorsFromMemory(id, 8, 42);
    assertEquals(4, store.findSubject(id).size(), () -> "flush failed! "+store.findSubject(id));

    store.flushAuthenticatorsFromMemory(id, 43, 43);
    assertEquals(3, store.findSubject(id).size(), () -> "flush failed! "+store.findSubject(id));
    } finally {
      env.destroy();
    }
  }
}
  class TestPeerReview implements PeerReview<MyInetSocketAddress, MyInetSocketAddress> {

    Environment env;
    AuthenticatorSerializer aSer;
    public TestPeerReview(Environment env, AuthenticatorSerializer aSer) {
      this.env = env;
      this.aSer = aSer;
    }

    public Serializer<MyInetSocketAddress> getIdSerializer() {
      return MyInetSocketAddress.serializer;
    }

    public Environment getEnvironment() {
      return env;
    }

    public AuthenticatorSerializer getAuthenticatorSerializer() {
      return aSer;
    }

    public long getTime() {
      return env.getTimeSource().currentTimeMillis();
    }

    public Serializer<MyInetSocketAddress> getHandleSerializer() {
      return null;
    }

    public int getHashSizeInBytes() {
      return 0;
    }

    public int getSignatureSizeInBytes() {
      return 0;
    }

    public IdentifierExtractor<MyInetSocketAddress, MyInetSocketAddress> getIdentifierExtractor() {
      return null;
    }

    public Authenticator extractAuthenticator(MyInetSocketAddress id, long seq,
        short entryType, byte[] entryHash, byte[] topMinusOne, byte[] signature) {
      return null;
    }

    public void challengeSuspectedNode(MyInetSocketAddress h) {

    }

    public long getEvidenceSeq() {
      return 0;
    }

    public void sendEvidenceToWitnesses(MyInetSocketAddress subject,
        long timestamp, Evidence evidence) {

    }

    public void notifyCertificateAvailable(MyInetSocketAddress id) {

    }

    public void incomingSocket(P2PSocket<MyInetSocketAddress> s)
        throws IOException {

    }

    public void messageReceived(MyInetSocketAddress i, ByteBuffer m,
        Map<String, Object> options) throws IOException {

    }

    public boolean hasCertificate(MyInetSocketAddress id) {
      return false;
    }

    public Cancellable requestCertificate(MyInetSocketAddress source,
        MyInetSocketAddress certHolder,
        Continuation<X509Certificate, Exception> c, Map<String, Object> options) {
      return null;
    }

    public byte[] sign(byte[] bytes) {
      return null;
    }

    public short getSignatureSizeBytes() {
      return 0;
    }

    public void verify(MyInetSocketAddress id, byte[] msg, int moff, int mlen,
        byte[] signature, int soff, int slen) throws InvalidKeyException,
        NoSuchAlgorithmException, NoSuchProviderException, SignatureException,
        UnknownCertificateException {

    }

    public void acceptMessages(boolean b) {

    }

    public void acceptSockets(boolean b) {

    }

    public MyInetSocketAddress getLocalIdentifier() {
      return null;
    }

    public SocketRequestHandle<MyInetSocketAddress> openSocket(
        MyInetSocketAddress i,
        SocketCallback<MyInetSocketAddress> deliverSocketToMe,
        Map<String, Object> options) {
      return null;
    }

    public MessageRequestHandle<MyInetSocketAddress, ByteBuffer> sendMessage(
        MyInetSocketAddress i, ByteBuffer m,
        MessageCallback<MyInetSocketAddress, ByteBuffer> deliverAckToMe,
        Map<String, Object> options) {
      return null;
    }

    public void setCallback(
        TransportLayerCallback<MyInetSocketAddress, ByteBuffer> callback) {

    }

    public void setErrorHandler(ErrorHandler<MyInetSocketAddress> handler) {

    }

    public void destroy() {

    }

    public byte[] getEmptyHash() {
      return null;
    }

    public short getHashSizeBytes() {
      return 0;
    }

    public byte[] hash(long seq, short type, byte[] nodeHash, byte[] contentHash) {
      return null;
    }

    public byte[] hash(ByteBuffer... hashMe) {
      return null;
    }

    public PeerReviewCallback<MyInetSocketAddress, MyInetSocketAddress> getApp() {
      return null;
    }

    public int verify(MyInetSocketAddress id, ByteBuffer msg,
        ByteBuffer signature) {
      return SIGNATURE_BAD;
    }

    public boolean addAuthenticatorIfValid(
        AuthenticatorStore<MyInetSocketAddress> store,
        MyInetSocketAddress subject, Authenticator auth) {
      return false;
    }

    public void transmit(
        MyInetSocketAddress dest, PeerReviewMessage message,
        MessageCallback<MyInetSocketAddress, ByteBuffer> deliverAckToMe,
        Map<String, Object> options) {
    }

    public EvidenceTool<MyInetSocketAddress, MyInetSocketAddress> getEvidenceTool() {
      return null;
    }

    public MyInetSocketAddress getLocalId() {
      return null;
    }

    public boolean verify(MyInetSocketAddress subject, Authenticator auth) {
      return false;
    }

    public MyInetSocketAddress getLocalHandle() {
      return null;
    }

    public RandomSource getRandomSource() {
      return null;
    }

    public SecureHistoryFactory getHistoryFactory() {
      return null;
    }

    public VerifierFactory<MyInetSocketAddress, MyInetSocketAddress> getVerifierFactory() {
      return null;
    }

    public void init(String dirname) throws IOException {

    }

    public void setApp(
        PeerReviewCallback<MyInetSocketAddress, MyInetSocketAddress> callback) {

    }

    public Cancellable requestCertificate(MyInetSocketAddress source,
        MyInetSocketAddress certHolder) {
      return null;
    }

    public SecureHistory getHistory() {
      return null;
    }

    public long getTimeToleranceMillis() {
      return 0;
    }

    public Authenticator extractAuthenticator(long seq, short entryType,
        byte[] entryHash, byte[] topMinusOne, byte[] signature) {
      return null;
    }

    public int verify(MyInetSocketAddress id, byte[] msg, byte[] signature) {
      return 0;
    }

    public void sendEvidence(MyInetSocketAddress destination,
        MyInetSocketAddress evidenceAgainst) {

    }
  }

  class TestAuthenticatorStore extends AuthenticatorStoreImpl<MyInetSocketAddress> {

    public TestAuthenticatorStore(PeerReview<?, MyInetSocketAddress> peerreview,
        boolean allowDuplicateSeqs) {
      super(peerreview, allowDuplicateSeqs);
    }

    @Override
    public void addAuthenticatorToMemory(MyInetSocketAddress id,
        Authenticator authenticator) {
      super.addAuthenticatorToMemory(id, authenticator);
    }

    @Override
    public void flushAuthenticatorsFromMemory(MyInetSocketAddress id,
        long minseq, long maxseq) {
      super.flushAuthenticatorsFromMemory(id, minseq, maxseq);
    }

    @Override
    public SortedSet<Authenticator> findSubject(MyInetSocketAddress id) {
      return super.findSubject(id);
    }


  }
