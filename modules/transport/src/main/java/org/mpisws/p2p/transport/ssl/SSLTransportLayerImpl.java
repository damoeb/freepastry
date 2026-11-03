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
package org.mpisws.p2p.transport.ssl;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.jspecify.annotations.Nullable;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import rice.Continuation;
import rice.environment.Environment;

/**
 * Does not encrypt UDP messages
 * The server authenticates to the client via a CACert
 *
 *
 * @author Jeff Hoye
 */
public class SSLTransportLayerImpl<Identifier, MessageType> implements SSLTransportLayer<Identifier, MessageType> {
  private final TransportLayer<Identifier, MessageType> tl;
  @SuppressWarnings("NullAway.Init") // set by setCallback() before the layer is used
  private TransportLayerCallback<Identifier, MessageType> callback;
  private ErrorHandler<Identifier> errorHandler;
  private final Logger logger;
  private final Environment environment;

  private final SSLContext context;

  private int clientAuth;

  public SSLTransportLayerImpl(TransportLayer<Identifier, MessageType> tl, KeyStore keyStore, KeyStore trustStore, Environment env) throws IOException {
    this(tl,keyStore,trustStore,CLIENT_AUTH_REQUIRED,env);
  }

  /**
   *
   * @param keyStore this node's keypair and certificate
   * @param trustStore the certificates to trust
   * @param clientAuth CLIENT_AUTH_NONE or CLIENT_AUTH_REQUIRED, used on the server side
   */
  public SSLTransportLayerImpl(TransportLayer<Identifier, MessageType> tl, KeyStore keyStore, KeyStore trustStore, int clientAuth, Environment env) throws IOException {
    if (clientAuth != CLIENT_AUTH_NONE && clientAuth != CLIENT_AUTH_REQUIRED) throw new IllegalArgumentException("clientAuth type:"+clientAuth+" not supported.");

    this.environment = env;
    this.logger = env.getLogManager().getLogger(SSLTransportLayerImpl.class, null);
    this.tl = tl;
    errorHandler = new DefaultErrorHandler<Identifier>(logger, Level.WARN);
    this.clientAuth = clientAuth;

    try {
      this.context = SSLContext.getInstance("TLS");
    char[] passphrase = "".toCharArray();

    KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
    kmf.init(keyStore, passphrase);

    TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
    tmf.init(trustStore);

    // always the configured trust managers: null would fall back to the JVM default trust store, even without client auth
    TrustManager[] tms = tmf.getTrustManagers();
    context.init(kmf.getKeyManagers(), tms, null);

    tl.setCallback(this);
    } catch (GeneralSecurityException gse) {
      throw new RuntimeException(gse);
    }

  }

  @Override
  public SocketRequestHandle<Identifier> openSocket(Identifier i,
      final SocketCallback<Identifier> deliverSocketToMe, @Nullable Map<String, Object> options) {
    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(i,options,logger);
    ret.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>() {

      @Override
      public void receiveException(SocketRequestHandle<Identifier> s,
          Exception ex) {
        deliverSocketToMe.receiveException(s, ex);
      }

      @Override
      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> sock) {
        getSocketManager(SSLTransportLayerImpl.this, sock, new Continuation<SSLSocketManager<Identifier>, Exception>() {

          @Override
          public void receiveException(Exception exception) {
            deliverSocketToMe.receiveException(ret, exception);
          }

          @Override
          public void receiveResult(SSLSocketManager<Identifier> result) {
            deliverSocketToMe.receiveResult(ret, result);
          }}, false, clientAuth != CLIENT_AUTH_NONE);
      }

    }, options));
    return ret;
  }

  /**
   * TODO: support SSL session resumption
   */
  @Override
  public void incomingSocket(final P2PSocket<Identifier> s) throws IOException {
    getSocketManager(this,s,new Continuation<SSLSocketManager<Identifier>, Exception>() {

      @Override
      public void receiveException(Exception exception) {
        errorHandler.receivedException(s.getIdentifier(), exception);
      }

      @Override
      public void receiveResult(SSLSocketManager<Identifier> result) {
        try {
          callback.incomingSocket(result);
        } catch (IOException ioe) {
          result.close();
          errorHandler.receivedException(s.getIdentifier(), ioe);
        }
      }},true, clientAuth != CLIENT_AUTH_NONE);
  }

  @Override
  public void setCallback(TransportLayerCallback<Identifier, MessageType> callback) {
    this.callback = callback;
  }

  @Override
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  @Override
  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  @Override
  public void destroy() {
    tl.destroy();
  }

  @Override
  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  @Override
  public @Nullable MessageRequestHandle<Identifier, MessageType> sendMessage(Identifier i,
      MessageType m, @Nullable MessageCallback<Identifier, MessageType> deliverAckToMe,
      @Nullable Map<String, Object> options) {
    return tl.sendMessage(i, m, deliverAckToMe, options);
  }

  @Override
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  @Override
  public void messageReceived(Identifier i, MessageType m,
      @Nullable Map<String, Object> options) throws IOException {
    callback.messageReceived(i, m, options);
  }

  protected SSLSocketManager<Identifier> getSocketManager(SSLTransportLayerImpl<Identifier, ?> sslTL,
      P2PSocket<Identifier> s,
      Continuation<SSLSocketManager<Identifier>, Exception> c, boolean server, boolean useClientAuth) {
    return new SSLSocketManager<Identifier>(sslTL,s,c,server,useClientAuth);
  }

  public SSLContext getContext() {
    return context;
  }

  public Environment getEnvironment() {
    return environment;
  }

  public Logger getLogger() {
    return logger;
  }
}
