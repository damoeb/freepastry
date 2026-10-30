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
package org.mpisws.p2p.transport.rc4;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.AuthenticationException;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.p2p.util.SecurityUtils;

/**
 * Only encrypts socket traffic!!!
 *
 * Encrypts channels based on a password, with AES-256 in CTR mode (it used to be RC4; the class keeps its name).
 *
 * Builds a session key for each direction of each socket: HMAC-SHA256 of a clear-text seed under the user-defined
 * password.  Each key is used for one direction of one socket only, so the CTR keystream starts at a zero IV.
 *
 * Note that CTR, like RC4, gives no integrity: an active attacker can flip bits of the plaintext undetected.  And
 * the seed of each direction is chosen by the receiving end, so a peer that replays a seed makes this end reuse
 * a keystream.
 *
 * Note that the seed length isn't sent in the protocol, so it must be the same on all peers.
 *
 * Why do we need a password seed?  Imagine a protocol where you contact a peer and it returns a boolean.
 * W/O a seed, the protocol would only return 2 possible messages.  While you may not know which is which, you could probably figure it
 * out by seeing who returns what.
 *
 * Both sides need to have a different seed, so that under a simple agreement protocol, you can't easily see a replay.
 *
 * E.G:
 *   A -> B: 14  // I propose 14
 *   B -> A: 14  // I agree to 14, in this case an eavesdropper could determine whether or not B agreed with A, which is a leak of info
 *
 * Here's the Seed protocol we use:
 *
 * // pw is a pre-established password
 * A -> B: SeedB
 * B -> E(H(pw,SeedB))[SeedA,SeedB]
 *  // Now A and B each have an agreed upon seed
 *  // Note that it may be important in step 2 to send SeedA before SeedB, otherwise, an attacker could keep sending new SeedB,
 *  // and maybe learn something about the first packet... but maybe this doesn't matter because of the Hashing to generate the secret key
 *
 * @author Jeff Hoye
 *
 * @param <Identifier>
 * @param <MsgType>
 */
public class RC4TransportLayer<Identifier, MsgType> implements TransportLayer<Identifier,MsgType>, TransportLayerCallback<Identifier, MsgType> {

  private final TransportLayer<Identifier,MsgType> tl;
  private TransportLayerCallback<Identifier, MsgType> callback;

  private final Environment env;
  private final Logger logger;

  private ErrorHandler<Identifier> errorHandler;

  /**
   * The default length of the seeds, in bytes.
   */
  public static final int KEY_LENGTH = 16;

  /**
   * The length of the CTR initial counter block, in bytes.
   */
  static final int IV_LENGTH = 16;

  byte[] password;
  int PW_SEED_LENGTH;
  int WRITE_BUFFER_SIZE = 1024;
  RandomSource random;

  public RC4TransportLayer(TransportLayer<Identifier,MsgType> tl, Environment env, String password, ErrorHandler<Identifier> errorHandler) throws NoSuchAlgorithmException {
    this(tl,env,password,KEY_LENGTH,env.getRandomSource(), errorHandler);
  }

  public RC4TransportLayer(TransportLayer<Identifier,MsgType> tl, Environment env, String password, int pwSeedLength, RandomSource random, ErrorHandler<Identifier> errorHandler) throws NoSuchAlgorithmException {
    this.tl = tl;
    this.tl.setCallback(this);
    this.env=env;
    if (password.isEmpty()) {
      throw new IllegalArgumentException("The password must not be empty");
    }
    this.password = password.getBytes(StandardCharsets.UTF_8);
    this.PW_SEED_LENGTH = pwSeedLength;
    this.logger=env.getLogManager().getLogger(RC4TransportLayer.class, null);
    this.random = random;
    this.errorHandler = errorHandler;
  }

  /**
   * @return The AES-256 key for the direction whose seed is given: HMAC-SHA256(password, seed)
   */
  static SecretKey sessionKey(byte[] password, byte[] seed) {
    return new SecretKeySpec(SecurityUtils.hmac(password, seed), KEY_ALGORITHM);
  }

  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  public SocketRequestHandle<Identifier> openSocket(final Identifier i,
      final SocketCallback<Identifier> deliverSocketToMe, final Map<String, Object> options) {

    // Every socket is encrypted. A per-socket option to turn encryption off would need the other end to learn that the socket is unencrypted.
    // TODO: send a byte saying whether the socket is encrypted and honor it on the accepting end, so encryption can be turned off per socket

    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(i,options,logger);

    ret.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>() {

      public void receiveException(SocketRequestHandle<Identifier> s,
          Exception ex) {
        deliverSocketToMe.receiveException(s, ex);
      }

      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> sock) {

        // write the decrypt seed to B
        final byte[] decryptSeed = new byte[PW_SEED_LENGTH];
        random.nextBytes(decryptSeed);

        P2PSocketReceiver<Identifier> writeDecryptSeedReceiver = new P2PSocketReceiver<Identifier>() {
          ByteBuffer dsBB = ByteBuffer.wrap(decryptSeed);

          public void receiveSelectResult(P2PSocket<Identifier> socket,
              boolean canRead, boolean canWrite) throws IOException {
            long bytesWritten = socket.write(dsBB);
            if (bytesWritten < 0) {
              socket.close();
              deliverSocketToMe.receiveException(ret, new ClosedChannelException("Socket to "+i+" closed by remote host. "+ret));
              return;
            }
            if (dsBB.hasRemaining()) {
              // need to finish writing
              socket.register(false, true, this);
              return;
            }

            // build the key
            SecretKey decryptKey = sessionKey(password, decryptSeed);
            try {
              final Cipher decryptCipher = Cipher.getInstance(ALGORITHM, PROVIDER);
              decryptCipher.init(Cipher.DECRYPT_MODE, decryptKey, new IvParameterSpec(new byte[IV_LENGTH]));
              // decrypt/read the response
              P2PSocketReceiver<Identifier> readEncryptSeedReceiver = new P2PSocketReceiver<Identifier>() {
                ByteBuffer encryptedSeeds = ByteBuffer.allocate(PW_SEED_LENGTH*2); // Because we have to verify that SeedB is properly encrypted
                public void receiveSelectResult(P2PSocket<Identifier> socket,
                    boolean canRead, boolean canWrite) throws IOException {
                  long bytesRead = socket.read(encryptedSeeds);
                  if (bytesRead < 0) {
                    socket.close();
                    deliverSocketToMe.receiveException(ret, new ClosedChannelException("Socket to "+i+" closed by remote host. "+ret));
                    return;
                  }
                  if (encryptedSeeds.hasRemaining()) {
                    // need to finish reading
                    socket.register(true, false, this);
                    return;
                  }

                  // decrypt the keys
                  byte[] decryptedSeeds = decryptCipher.update(encryptedSeeds.array());

                  // make sure decryptSeed == the second key in decryptedSeeds
                  boolean same = true;
                  for (int ctr = 0; ctr < PW_SEED_LENGTH; ctr++) {
                    if (decryptedSeeds[ctr+PW_SEED_LENGTH] != decryptSeed[ctr]) {
                      same = false;
                      break;
                    }
                  }
                  if (!same) {
                    socket.close();
                    deliverSocketToMe.receiveException(ret, new AuthenticationException("Counterpart failed to properly encrypt his encryptSeed in the message."));
                    return;
                  }

                  // copy out the encryptSeed
                  byte[] encryptSeed = new byte[PW_SEED_LENGTH];
                  System.arraycopy(decryptedSeeds, 0, encryptSeed, 0, PW_SEED_LENGTH);

                  // build the encrypt key
                  SecretKey encryptKey = sessionKey(password, encryptSeed);
                  try {
                    Cipher encryptCipher = Cipher.getInstance(ALGORITHM, PROVIDER);
                    encryptCipher.init(Cipher.ENCRYPT_MODE, encryptKey, new IvParameterSpec(new byte[IV_LENGTH]));
                    deliverSocketToMe.receiveResult(ret, new EncryptedSocket<Identifier>(i,socket,logger,errorHandler,options,encryptCipher,decryptCipher,WRITE_BUFFER_SIZE));
                  } catch (GeneralSecurityException gse) {
                    socket.close();
                    deliverSocketToMe.receiveException(ret, gse);
                  }
                }

                public void receiveException(P2PSocket<Identifier> socket,
                    Exception ioe) {
                  deliverSocketToMe.receiveException(ret, ioe);
                }
              };
              readEncryptSeedReceiver.receiveSelectResult(socket, true, false);
            } catch (GeneralSecurityException gse) {
              socket.close();
              deliverSocketToMe.receiveException(ret, gse);
            }
          }

          public void receiveException(P2PSocket<Identifier> socket,
              Exception ioe) {
            deliverSocketToMe.receiveException(ret, ioe);
          }
        };
        try {
          writeDecryptSeedReceiver.receiveSelectResult(sock, false, true);
        } catch (IOException ioe) {
          deliverSocketToMe.receiveException(ret, ioe);
        }
      }
    }, options));

    return ret;
  }

  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
    // read the encryptSeed in plaintext
    // generate the encryptCipher
    // generate a decryptSeed
    // generate the decryptCipher
    // encrypt the decryptSeed+encryptSeed
    // send them
    // build the Encrypted Socket

    new P2PSocketReceiver<Identifier>() {
      byte[] encryptSeed = new byte[PW_SEED_LENGTH];
      ByteBuffer encryptSeedBuffer = ByteBuffer.wrap(encryptSeed);

      public void receiveSelectResult(P2PSocket<Identifier> socket,
          boolean canRead, boolean canWrite) throws IOException {
        // read the encryptSeedBuffer until it is full
        long ret = socket.read(encryptSeedBuffer);
        if (ret < 0) {
          socket.close();
        }
        if (encryptSeedBuffer.hasRemaining()) {
          socket.register(true, false, this);
          return;
        }

        // build the encrypt key
        SecretKey encryptKey = sessionKey(password, encryptSeed);
        try {
          final Cipher encryptCipher = Cipher.getInstance(ALGORITHM, PROVIDER);
          encryptCipher.init(Cipher.ENCRYPT_MODE, encryptKey, new IvParameterSpec(new byte[IV_LENGTH]));

          // write the decrypt seed to A
          final byte[] decryptSeed = new byte[PW_SEED_LENGTH];
          random.nextBytes(decryptSeed);

          // build the key
          SecretKey decryptKey = sessionKey(password, decryptSeed);
          final Cipher decryptCipher = Cipher.getInstance(ALGORITHM, PROVIDER);
          decryptCipher.init(Cipher.DECRYPT_MODE, decryptKey, new IvParameterSpec(new byte[IV_LENGTH]));

          // encrypt the seeds
          byte[] encryptedSeeds = new byte[PW_SEED_LENGTH*2];
          encryptCipher.update(decryptSeed,0,PW_SEED_LENGTH,encryptedSeeds,0);
          encryptCipher.update(encryptSeed,0,PW_SEED_LENGTH,encryptedSeeds,PW_SEED_LENGTH);

          // write the encrypted seeds
          final ByteBuffer encryptedSeedsBuffer = ByteBuffer.wrap(encryptedSeeds);
          new P2PSocketReceiver<Identifier>() {

            public void receiveSelectResult(P2PSocket<Identifier> socket,
                boolean canRead, boolean canWrite) throws IOException {
              long ret = socket.write(encryptedSeedsBuffer);
              if (ret < 0) {
                receiveException(socket, new ClosedChannelException("Channel closed while establishing crypto. "+socket));
                return;
              }
              if (encryptedSeedsBuffer.hasRemaining()) {
                socket.register(false, true, this);
                return;
              }

              // we're done...
              callback.incomingSocket(new EncryptedSocket<Identifier>(socket.getIdentifier(),socket,logger,errorHandler,socket.getOptions(),encryptCipher,decryptCipher,WRITE_BUFFER_SIZE));
            }

            public void receiveException(P2PSocket<Identifier> socket,
                Exception ioe) {
              errorHandler.receivedException(socket.getIdentifier(), ioe);
              socket.close();
            }
          }.receiveSelectResult(socket, false, true);


        } catch (GeneralSecurityException gse) {
          socket.close();
        }

      }

      public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
        errorHandler.receivedException(socket.getIdentifier(), ioe);
        socket.close();
      }

    }.receiveSelectResult(s,true,false);
  }

  public MessageRequestHandle<Identifier, MsgType> sendMessage(Identifier i,
      MsgType m, MessageCallback<Identifier, MsgType> deliverAckToMe,
      Map<String, Object> options) {
    return tl.sendMessage(i, m, deliverAckToMe, options);
  }

  public void setCallback(TransportLayerCallback<Identifier, MsgType> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  public void destroy() {

  }

  public void messageReceived(Identifier i, MsgType m,
      Map<String, Object> options) throws IOException {
    callback.messageReceived(i, m, options);
  }

  private static final String KEY_ALGORITHM = "AES";
  static final String ALGORITHM = KEY_ALGORITHM + "/CTR/NoPadding";

  /**
   * The JDK's CTR returns every byte from update() at once, as {@link EncryptedSocket} needs.  BouncyCastle's,
   * which SecurityUtils puts ahead of it in the provider list, holds back a partial block.
   */
  static final String PROVIDER = "SunJCE";
}
