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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mpisws.p2p.pki.x509.CATool;
import org.mpisws.p2p.pki.x509.CATool;
import org.mpisws.p2p.pki.x509.X509Serializer;
import org.mpisws.p2p.pki.x509.X509Serializer;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.peerreview.IdentifierExtractor;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.StatusConstants;
import org.mpisws.p2p.transport.peerreview.WitnessListener;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA256HashProvider;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransportLayerImpl;
import org.mpisws.p2p.transport.peerreview.infostore.IdStrTranslator;
import org.mpisws.p2p.transport.peerreview.replay.Verifier;
import org.mpisws.p2p.transport.peerreview.replay.record.RecordLayer;
import org.mpisws.p2p.transport.util.Serializer;
import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.p2p.util.tuples.Tuple;
import rice.selector.TimerTask;

import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_TRUSTED;

public class PRRegressionTest {
  public static final byte[] EMPTY_ARRAY = new byte[0];
  static class IdExtractor implements IdentifierExtractor<HandleImpl, IdImpl> {

    @Override
    public IdImpl extractIdentifier(HandleImpl h) {
      return h.id;
    }

  }

  static class HandleSerializer implements Serializer<HandleImpl> {

    @Override
    public HandleImpl deserialize(InputBuffer buf) throws IOException {
      return HandleImpl.build(buf);
    }

    @Override
    public void serialize(HandleImpl i, OutputBuffer buf) throws IOException {
      i.serialize(buf);
    }

  }

  static class IdSerializer implements Serializer<IdImpl> {

    @Override
    public IdImpl deserialize(InputBuffer buf) throws IOException {
      return IdImpl.build(buf);
    }

    @Override
    public void serialize(IdImpl i, OutputBuffer buf) throws IOException {
      i.serialize(buf);
    }

  }

  static class HandleImpl implements RawSerializable {
    String name;
    IdImpl id;

    public HandleImpl(String s, IdImpl id) {
      this.name = s;
      this.id = id;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeUTF(name);
      id.serialize(buf);
    }

    public static HandleImpl build(InputBuffer buf) throws IOException {
      return new HandleImpl(buf.readUTF(), IdImpl.build(buf));
    }

    @Override
    public String toString() {
      return "HandleImpl<"+name+">";
    }

    @Override
    public int hashCode() {
      return id.hashCode()^name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof HandleImpl that)) return false;
      if (!id.equals(that.id)) return false;
      return name.equals(that.name);
    }
  }

  static class IdImpl implements RawSerializable {
    int id;
    public IdImpl(int id) {
      this.id = id;
    }
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(id);
    }

    public static IdImpl build(InputBuffer buf) throws IOException {
      return new IdImpl(buf.readInt());
    }

    @Override
    public String toString() {
      return "Id<"+id+">";
    }

    @Override
    public int hashCode() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof IdImpl that)) return false;
      return (this.id == that.id);
    }
  }


  /**
   * The in-memory "network": delivers each message straight to the destination's selector.
   */
  Map<HandleImpl, BogusTransport> peerTable = new HashMap<HandleImpl, BogusTransport>();

  class BogusTransport implements TransportLayer<HandleImpl, ByteBuffer> {

    HandleImpl localIdentifier;
    Environment env;

    public BogusTransport(HandleImpl handle, X509Certificate cert, Environment env) {
      peerTable.put(handle, this);
      this.localIdentifier = handle;
      this.env = env;
    }

    @Override
    public void acceptMessages(boolean b) {
      throw new RuntimeException("implement");
    }

    @Override
    public void acceptSockets(boolean b) {
      throw new RuntimeException("implement");
    }

    @Override
    public HandleImpl getLocalIdentifier() {
      return localIdentifier;
    }

    @Override
    public SocketRequestHandle<HandleImpl> openSocket(HandleImpl i,
        SocketCallback<HandleImpl> deliverSocketToMe,
        Map<String, Object> options) {
      throw new RuntimeException("implement");
    }

    @Override
    public MessageRequestHandle<HandleImpl, ByteBuffer> sendMessage(
        HandleImpl i, ByteBuffer m,
        MessageCallback<HandleImpl, ByteBuffer> deliverAckToMe,
        Map<String, Object> options) {
      peerTable.get(i).receiveMessage(localIdentifier, m);
      return null;
    }

    TransportLayerCallback<HandleImpl, ByteBuffer> callback;

    /**
     * Delivers the message as a timer task due now, not as an invocation. The RecordSM runs pending
     * invocations at the time of a due timer but before that timer, while the replay runs every timer due
     * at or before a logged event first; a message delivered by invocation while one of the app's timers
     * was due would thus be replayed in the wrong order, and the auditor would expose a correct node. A
     * real transport reads its messages in the select phase, after the due timers.
     */
    private void receiveMessage(final HandleImpl i, final ByteBuffer m) {
      // schedule on the selector thread, whose simulated clock does not move under us
      env.getSelectorManager().invoke(() -> env.getSelectorManager().schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            callback.messageReceived(i, m, null);
          } catch (IOException ioe) {
            logger.warn("Error delivering a message to {}", localIdentifier, ioe);
            fail("Error delivering a message to "+localIdentifier+": "+ioe);
          }
        }
      }, 0));
    }

    @Override
    public void setCallback(
        TransportLayerCallback<HandleImpl, ByteBuffer> callback) {
      this.callback = callback;
    }

    @Override
    public void setErrorHandler(ErrorHandler<HandleImpl> handler) {
      throw new RuntimeException("implement");
    }

    @Override
    public void destroy() {
      throw new RuntimeException("implement");
    }
  }

  class BogusApp implements PeerReviewCallback<HandleImpl, IdImpl> {

    Logger logger;
    Player player;
    Environment env;
    Random rand;
    long nextSendTime = 0;
    private HandleImpl dest;

    private TransportLayer<HandleImpl, ByteBuffer> tl;

    public BogusApp(Player player, TransportLayer<HandleImpl, ByteBuffer> tl, Environment env) {
      super();
      this.player = player;
      this.tl = tl;
      this.env = env;
      this.logger = env.getLogManager().getLogger(BogusApp.class, null);
      logger.info("new bogus app {}", player);
      dest = player.destHandle;
      tl.setCallback(this);
    }


    @Override
    public void init() {
      rand = new Random();
      if (player.localHandle.id.id == 1) {
        // need to give the other nodes a second to boot, otherwise this message will go nowhere
        scheduleMessageToBeSent(env.getTimeSource().currentTimeMillis()+1000, true);
      }
    }

    public void scheduleMessageToBeSent() {
      scheduleMessageToBeSent(env.getTimeSource().currentTimeMillis()+rand.nextInt(1999)+1, true);
    }

    public void scheduleMessageToBeSent(long time, final boolean reschedule) {
      nextSendTime = time;
      logger.debug("scheduling message to be sent at:{}", time);
      env.getSelectorManager().schedule(new TimerTask() {
        @Override
        public String toString() {
          return "SendMessageTask "+scheduledExecutionTime();
        }

        @Override
        public void run() {
          sendMessage();
          if (reschedule) scheduleMessageToBeSent();
        }
      }, nextSendTime-env.getTimeSource().currentTimeMillis());
    }

    public void sendMessage() {
      byte[] msg = generateMessage();

      logger.atInfo().log(() -> "sending message "+msg.length+" "+MathUtils.toBase64(msg));
      try {
      tl.sendMessage(dest, ByteBuffer.wrap(msg), new MessageCallback<HandleImpl, ByteBuffer>() {

        @Override
        public void sendFailed(MessageRequestHandle<HandleImpl, ByteBuffer> msg,
            Exception reason) {
          logger.info("sendFailed({})", msg);
        }

        @Override
        public void ack(MessageRequestHandle<HandleImpl, ByteBuffer> msg) {
          logger.debug("ack({}) {}", msg, Thread.currentThread());
        }

      }, null);
      } catch (NullPointerException npe) {
        logger.info("tl:{} {}", tl, dest);
        throw npe;
      }
    }

    protected byte[] generateMessage() {
      byte[] msg = new byte[rand.nextInt(31)+1];
      rand.nextBytes(msg);
      return msg;
    }

    @Override
    public void storeCheckpoint(OutputBuffer buffer) throws IOException {
      logger.trace("storeCheckpoint {}", nextSendTime);
      buffer.writeInt(31173);
      buffer.writeLong(nextSendTime);
      buffer.writeBoolean(dest != null);
      if (dest != null) dest.serialize(buffer);
      // reseed, so that the generator's state is just the seed the checkpoint keeps
      long seed = rand.nextLong();
      rand = new Random(seed);
      buffer.writeLong(seed);
      logger.atTrace().log(() -> "storeCheckpoint:"+Arrays.toString(((SimpleOutputBuffer)buffer).getBytes()));
    }

    @Override
    public boolean loadCheckpoint(InputBuffer buffer) throws IOException {
      if (buffer.readInt() != 31173) throw new RuntimeException("invalid checkpoint");
      nextSendTime = buffer.readLong();
      if (buffer.readBoolean()) {
        dest = new HandleSerializer().deserialize(buffer);
      }
      logger.trace("loadCheckpoint {}", nextSendTime);
      rand = new Random(buffer.readLong());
      if (nextSendTime > 0) {
        scheduleMessageToBeSent(nextSendTime, true);
      }
      return true;
    }

    @Override
    public void destroy() {
      throw new RuntimeException("implement");
    }

    @Override
    public void notifyCertificateAvailable(IdImpl id) {
      throw new RuntimeException("implement");
    }

    public void receive(HandleImpl source, boolean datagram, ByteBuffer msg) {
      throw new RuntimeException("implement");
    }

    public void sendComplete(long id) {
      throw new RuntimeException("implement");
    }

    @Override
    public void incomingSocket(P2PSocket<HandleImpl> s) throws IOException {
      throw new RuntimeException("implement");
    }

    @Override
    public void messageReceived(HandleImpl i, ByteBuffer m,
        Map<String, Object> options) throws IOException {
      logger.atInfo().log(() -> "Message received: "+MathUtils.toBase64(Arrays.copyOfRange(m.array(), m.arrayOffset()+m.position(), m.arrayOffset()+m.limit())));
    }

    @Override
    public void getWitnesses(IdImpl subject,
        WitnessListener<HandleImpl, IdImpl> callback) {
      callback.notifyWitnessSet(subject, Collections.singletonList(carol.localHandle));
    }

    @Override
    public void notifyStatusChange(
        IdImpl id,
        int newStatus) {
      logger.atInfo().log(() -> "notifyStatusChange("+id+","+PeerReviewImpl.getStatusString(newStatus)+")");
      if (newStatus != STATUS_TRUSTED) {
        logger.info("Failure, Node not trusted: {} at {}", id, player.localHandle);
        fail("Node not trusted: "+id+" at "+player.localHandle);
        return;
      }
      addStatusNotification(this.player.localHandle,id,newStatus);
    }

    @Override
    public Collection<HandleImpl> getMyWitnessedNodes() {
      return player.witnessed;
    }

    @Override
    public PeerReviewCallback<HandleImpl, IdImpl> getReplayInstance(Verifier<HandleImpl, IdImpl> v) {
      BogusApp ret = new BogusApp(playerTable.get(v.getLocalIdentifier()),v,v.getEnvironment());
      return ret;
    }
  }

  Map<HandleImpl, IdentityTransportLayerImpl<HandleImpl, IdImpl>> idTLTable = new HashMap<HandleImpl, IdentityTransportLayerImpl<HandleImpl,IdImpl>>();

  CATool caTool;

  Map<HandleImpl,Player> playerTable = new HashMap<HandleImpl, Player>();

  protected PeerReviewImpl<HandleImpl, IdImpl> getPeerReview(Player player, MyIdTL transport, Environment env) {
    return new PeerReviewImpl<HandleImpl, IdImpl>(transport, env, new HandleSerializer(), new IdSerializer(), new IdExtractor(), getIdStrTranslator()
        );
  }


  class Player {
    Logger logger;
    HandleImpl localHandle;

    PeerReview<HandleImpl, IdImpl> pr;
    MyIdTL transport;
    public Collection<HandleImpl> witnessed = new ArrayList<HandleImpl>();
    KeyPair pair;
    X509Certificate cert;
    BogusTransport t1;
    Environment env;

    BogusApp app;
    HandleImpl destHandle;

    public Player(HandleImpl localHandle, HandleImpl dstHandle, final Environment env2) throws Exception {
      super();
      this.destHandle = dstHandle;
      env = cloneEnvironment(env2, localHandle.name, localHandle.id.id);

      this.logger = env.getLogManager().getLogger(Player.class, null);

      final File f = new File(dir, localHandle.name);
      if (f.exists()) {
        File f2 = new File(f,"peers");
        File[] foo = f2.listFiles();
        if (foo != null) {
          for (File file : foo) {
            file.delete();
          }
        }

        foo = f.listFiles();
        if (foo != null) {
          for (File file : foo) {
            file.delete();
          }
        }

      }

      this.localHandle = localHandle;
      playerTable.put(localHandle, this);

      // every node must use the same key size: the signature size is derived from the local certificate's key
      pair = CATool.generateKeyPair();
      cert = caTool.sign(localHandle.name,pair.getPublic());

      t1 = getTL();

      transport = getIdTransport();

      idTLTable.put(localHandle, transport);
      pr = getPeerReview(this, transport, env);
      app = getApp();
      pr.setApp(app);
      env.getSelectorManager().invoke(() -> {
        try {
          pr.init(f.getPath());
        } catch (IOException ioe) {
          logger.warn("Error initializing PeerReview of {}", Player.this.localHandle, ioe);
          fail("Error initializing PeerReview of "+Player.this.localHandle+": "+ioe);
        }
      });
    }

    public BogusApp getApp() {
      return getBogusApp(this, pr, env);
    }

    // to be overridden
    public Environment cloneEnvironment(Environment env2, String name, int id) {
      return env2.cloneEnvironment(name);
    }


    public void buildPlayerCryptoStuff() {

    }

    public BogusTransport getTL() throws Exception {
      return new BogusTransport(localHandle, cert, env);
    }

    public MyIdTL getIdTransport() throws Exception {
      return new MyIdTL(
          new IdSerializer(), new X509Serializer(),this.localHandle.id,
          cert,pair.getPrivate(),t1,new SHA256HashProvider(),env) {
      };
    }


  }

  public IdStrTranslator<IdImpl> getIdStrTranslator() {
    return new IdStrTranslator<IdImpl>(){

      @Override
      public IdImpl readIdentifierFromString(String s) {
        return new IdImpl(Integer.parseInt(s));
      }

      @Override
      public String toString(IdImpl id) {
        return Integer.toString(id.id);
      }};
  }

  class MyIdTL extends IdentityTransportLayerImpl<HandleImpl, IdImpl> {
    public MyIdTL(Serializer<IdImpl> serializer, X509Serializer serializer2,
        IdImpl localId, X509Certificate localCert, PrivateKey localPrivate,
        TransportLayer<HandleImpl, ByteBuffer> tl, HashProvider hasher,
        Environment env) throws InvalidKeyException, NoSuchAlgorithmException,
        NoSuchProviderException {
      super(serializer, serializer2, localId, localCert, localPrivate, tl, hasher,
          env);
    }

    @Override
    public Cancellable requestCertificate(final HandleImpl source, final IdImpl certHolder,
        final Continuation<X509Certificate, Exception> c, Map<String, Object> options) {
      return idTLTable.get(source).requestValue(source, certHolder, new Continuation<X509Certificate, Exception>() {

        @Override
        public void receiveResult(X509Certificate result) {
          knownValues.put(certHolder, result);
          if (c != null) c.receiveResult(result);
        }

        @Override
        public void receiveException(Exception exception) {
          if (c != null) c.receiveException(exception);
        }

      }, options);
    }
  }

  private HashProvider hasher;
  @SuppressWarnings("UnusedVariable") // assigned in setUp but not read by any scenario yet
  private SecureHistoryFactory historyFactory;

  Player alice;
  Player bob;
  Player carol;

  HandleImpl aliceHandle = new HandleImpl("alice", new IdImpl(1));
  HandleImpl bobHandle = new HandleImpl("bob", new IdImpl(2));
  HandleImpl carolHandle = new HandleImpl("carol", new IdImpl(3));


  protected BogusApp getBogusApp(Player player, PeerReview<HandleImpl, IdImpl> pr,
      Environment env) {
    return new BogusApp(player,pr,env);
  }

  public void buildCryptoMaterial(Environment env) throws Exception {
    // an in-memory CA, valid for a day (CATool.getCATool() would keep it in a key store file in the working directory)
    Tuple<X509Certificate, KeyPair> ca = CATool.generateNewCA("CommitmentTest", Instant.now().plus(Duration.ofDays(1)));
    caTool = new CATool(ca.first(), ca.second());
    hasher = new SHA256HashProvider();
    historyFactory = new SecureHistoryFactory(hasher,env);
  }

  public void buildPlayers(Environment env) throws Exception {
    alice = new Player(aliceHandle, bobHandle, env);
    bob = new Player(bobHandle, aliceHandle, env);
    carol = new Player(carolHandle, null, env);
  }

  public void setupWitnesses() {
    carol.witnessed.add(alice.localHandle);
    carol.witnessed.add(bob.localHandle);
  }

  /**
   * The last status each node was notified of, per node (written by the nodes' selector threads)
   */
  protected Map<HandleImpl, Map<IdImpl, Integer>> recordedStatus = new ConcurrentHashMap<HandleImpl, Map<IdImpl,Integer>>();
  protected void addStatusNotification(HandleImpl localHandle, IdImpl id,
      int newStatus) {
    recordedStatus.computeIfAbsent(localHandle, h -> new ConcurrentHashMap<IdImpl, Integer>()).put(id,newStatus);
  }

  /**
   * Called after millisToFinish (simulated) milliseconds: checks the outcome and calls {@link #succeed()} or
   * {@link #fail(String)}. This scenario expects every node to trust every other node.
   */
  public void finish() {
    for (Entry<HandleImpl, Map<IdImpl, Integer>> foo : recordedStatus.entrySet()) {
      for (Entry<IdImpl, Integer> i : foo.getValue().entrySet()) {
        if (i.getValue() != StatusConstants.STATUS_TRUSTED) {
          logger.info("Fail: {} found {} {}", foo.getKey(), i.getKey(), i.getValue());
          fail(foo.getKey()+" found "+i.getKey()+" "+PeerReviewImpl.getStatusString(i.getValue()));
          return;
        }
      }
    }
    logger.info("Success");
    succeed();
  }

  private final CountDownLatch done = new CountDownLatch(1);
  private volatile String failure;

  /**
   * Records the (first) failure and ends the scenario.
   */
  protected void fail(String reason) {
    synchronized (done) {
      if (failure == null) failure = reason;
    }
    done.countDown();
  }

  /**
   * Ends the scenario successfully (unless it failed before).
   */
  protected void succeed() {
    done.countDown();
  }

  /**
   * Waits for the scenario's outcome, then destroys its environment.
   *
   * @throws AssertionError if the scenario failed or did not finish in time
   */
  public void await(long timeoutMillis) throws InterruptedException {
    try {
      if (!done.await(timeoutMillis, TimeUnit.MILLISECONDS))
        throw new AssertionError(getClass().getSimpleName()+" did not finish within "+timeoutMillis+" ms");
      synchronized (done) {
        if (failure != null) throw new AssertionError(getClass().getSimpleName()+": "+failure);
      }
    } finally {
      env.destroy();
    }
  }

  /**
   * @return the PeerReview identity transport of each node
   */
  public Collection<IdentityTransportLayerImpl<HandleImpl, IdImpl>> getIdentityTransports() {
    return idTLTable.values();
  }

  /**
   * Runs the scenario as a program: exits with 0 on success, 1 otherwise.
   */
  @SuppressWarnings("CatchAndPrintStackTrace") // as a program, stderr is the output
  protected static void runMain(PRRegressionTest test) {
    int status = 1;
    try {
      test.await(MAIN_TIMEOUT_MILLIS);
      status = 0;
    } catch (Throwable t) {
      t.printStackTrace();
    }
    System.exit(status);
  }

  /**
   * How long the scenarios run (simulated time, which runs at wall-clock speed here) before {@link #finish()}.
   */
  public static final int DEFAULT_MILLIS_TO_FINISH = 45000;
  static final long MAIN_TIMEOUT_MILLIS = 3L*DEFAULT_MILLIS_TO_FINISH;

  Environment env;
  Logger logger;
  /**
   * The directory below which each node keeps its PeerReview files (in a subdirectory named after the node).
   */
  final File dir;

  /**
   * @param millisToFinish Call finish() after this amount of time
   */
  public PRRegressionTest(int millisToFinish) throws Exception {
    this(millisToFinish, new File("."));
  }

  /**
   * @param millisToFinish Call finish() after this amount of time
   * @param dir the directory for the nodes' PeerReview files
   */
  public PRRegressionTest(int millisToFinish, File dir) throws Exception {
    this.dir = dir;
    env = RecordLayer.generateEnvironment();
    env.getSelectorManager().schedule(new TimerTask() {

      @Override
      public void run() {
        finish();
      }
    },millisToFinish);

    logger = env.getLogManager().getLogger(PRRegressionTest.class, null);

    buildCryptoMaterial(env);

    env.getSelectorManager().invoke(() -> {
      try {
        buildPlayers(env);

        setupWitnesses();
      } catch (Exception ioe) {
        logger.warn("Error building the players", ioe);
        fail("Error building the players: "+ioe);
      }
    });
  }

  public static void main(String[] args) throws Exception {
    runMain(new PRRegressionTest(DEFAULT_MILLIS_TO_FINISH));
  }
}
