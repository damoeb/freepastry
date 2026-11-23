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


package rice.pastry;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.slf4j.Logger;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.p2p.commonapi.exception.AppNotRegisteredException;
import rice.p2p.commonapi.exception.AppSocketException;
import rice.p2p.commonapi.exception.NoReceiverAvailableException;
import rice.pastry.client.PastryAppl;
import rice.pastry.transport.SocketAdapter;

/**
 * Both ends of the handshake that opens an application socket between two nodes. The connector writes the 4-byte
 * address of the application and reads one answer byte ({@link #CONNECTION_OK}, {@link #CONNECTION_NO_APP} or
 * {@link #CONNECTION_NO_ACCEPTOR}); the acceptor reads the address, finds the application and writes the answer.
 * After an ok both sides hand the socket to their application.
 */
final class AppSocketHandshake {
  public static final byte CONNECTION_OK = 0;
  public static final byte CONNECTION_NO_APP = 1;
  public static final byte CONNECTION_NO_ACCEPTOR = 2;

  private final PastryNode node;
  private final Logger logger;

  AppSocketHandshake(PastryNode node, Logger logger) {
    this.node = node;
    this.logger = logger;
  }

  /**
   * Called by PastryAppl to ask the transport layer to open a Socket to its counterpart on another node.
   *
   * @param i the node to connect to
   * @param deliverSocketToMe receives the socket, or the exception
   * @param appl the application whose counterpart to connect to
   * @param timeout currently ignored
   * @return a handle to cancel the request
   */
  SocketRequestHandle<NodeHandle> connect(final NodeHandle i, final AppSocketReceiver deliverSocketToMe,
      final PastryAppl appl, int timeout) {

    final SocketRequestHandleImpl<NodeHandle> handle = new SocketRequestHandleImpl<NodeHandle>(i, null, logger);

    Runnable task = new Runnable() {
      @Override
      public void run() {

        // use the proper application address
        final ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.asIntBuffer().put(appl.getAddress());
        buffer.clear();


        handle.setSubCancellable(node.transportLayer().openSocket(i,
          new SocketCallback<NodeHandle>(){
            @Override
            @SuppressWarnings("ReferenceEquality") // the same instance is meant
            public void receiveResult(SocketRequestHandle<NodeHandle> c,
                P2PSocket<NodeHandle> result) {

              if (c != handle.getSubCancellable()) throw new RuntimeException("c != handle.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+handle.getSubCancellable());

              logger.trace("openSocket({}):receiveResult({})", i, result);
              result.register(false, true, new P2PSocketReceiver<NodeHandle>() {
                @Override
                public void receiveSelectResult(P2PSocket<NodeHandle> socket,
                    boolean canRead, boolean canWrite) throws IOException {
                  if (canRead || !canWrite) throw new IOException("Expected to write! "+canRead+","+canWrite);

                  // write the appId
                  if (socket.write(buffer) == -1) {
                    deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, node.getEnvironment()), new ClosedChannelException("Remote node closed socket while opening.  Try again."));
                    return;
                  }

                  // keep working or pass up the new socket
                  if (buffer.hasRemaining()) {
                    // keep writing
                    socket.register(false, true, this);
                  } else {
                    // read the response
                    final ByteBuffer answer = ByteBuffer.allocate(1);
                    socket.register(true, false, new P2PSocketReceiver<NodeHandle>(){

                      @Override
                      public void receiveSelectResult(P2PSocket<NodeHandle> socket, boolean canRead, boolean canWrite) throws IOException {

                        if (socket.read(answer) == -1) {
                          deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, node.getEnvironment()), new ClosedChannelException("Remote node closed socket while opening.  Try again."));
                          return;
                        };

                        if (answer.hasRemaining()) {
                          socket.register(true, false, this);
                        } else {
                          answer.clear();

                          byte connectResult = answer.get();
                          switch(connectResult) {
                            case CONNECTION_OK:
                              // on connector side
                              deliverSocketToMe.receiveSocket(new SocketAdapter<NodeHandle>(socket, node.getEnvironment()));
                              return;
                            case CONNECTION_NO_APP:
                              deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, node.getEnvironment()), new AppNotRegisteredException(appl.getAddress()));
                              return;
                            case CONNECTION_NO_ACCEPTOR:
                              deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, node.getEnvironment()), new NoReceiverAvailableException());
                              return;
                            default:
                              deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, node.getEnvironment()), new AppSocketException("Unknown error "+connectResult));
                              return;
                          }
                        }
                      }

                      @Override
                      public void receiveException(P2PSocket<NodeHandle> socket, Exception ioe) {
                        deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, node.getEnvironment()), ioe);
                      }
                    });
                  }
                }

                @Override
                public void receiveException(P2PSocket<NodeHandle> socket,
                    Exception e) {
                  deliverSocketToMe.receiveException(new SocketAdapter<NodeHandle>(socket, node.getEnvironment()), e);
                }
              });
            }

            @Override
            public void receiveException(SocketRequestHandle<NodeHandle> requestHandle, Exception ex) {
              // TODO: return something with a proper toString()
              deliverSocketToMe.receiveException(null, ex);
            }
          },
        null));
      }
    };
    if (node.getEnvironment().getSelectorManager().isSelectorThread()) {
      task.run();
    } else {
      node.getEnvironment().getSelectorManager().invoke(task);
    }


    return handle;
  }

  void incomingSocket(P2PSocket<NodeHandle> s) throws IOException {

    // read the appId
    final ByteBuffer appIdBuffer = ByteBuffer.allocate(4);

    s.register(true, false, new P2PSocketReceiver<NodeHandle>() {

      @Override
      public void receiveSelectResult(
          P2PSocket<NodeHandle> socket,
          boolean canRead, boolean canWrite) throws IOException {
        // read the appId
        if (socket.read(appIdBuffer) == -1) {
          logger.warn("AppId Socket from {} closed unexpectedly.", socket);
          return;
        }

        if (appIdBuffer.hasRemaining()) {
          // read the rest;
          socket.register(true, false, this);
        } else {
          appIdBuffer.clear();
          final int appId = appIdBuffer.asIntBuffer().get();

          // we need to write the result, and there is a timing issue on the appl, so we need to first request to write, then do everything
          // the alternative approach is to return a dummy socket (or a wrapper) and cache any registration request until we write the response
          socket.register(false, true, new P2PSocketReceiver<NodeHandle>(){

            @Override
            public void receiveSelectResult(P2PSocket<NodeHandle> socket,
                boolean canRead, boolean canWrite) throws IOException {

              PastryAppl acceptorAppl = node.getMessageDispatch().getDestinationByAddress(appId);

              ByteBuffer toWrite = ByteBuffer.allocate(1);
              boolean success = false;

              if (acceptorAppl == null) {
                logger.atWarn().log(() -> "Sending error to connecter "+socket+" "+new AppNotRegisteredException(appId));
                toWrite.put(CONNECTION_NO_APP);
                toWrite.clear();
                socket.write(toWrite);
                socket.close();
              } else {
                synchronized(acceptorAppl) {
                // try to register with the application
                  if (acceptorAppl.canReceiveSocket()) {
                    toWrite.put(CONNECTION_OK);
                    toWrite.clear();
                    success = true;
                  } else {
                    logger.atWarn().log(() -> "Sending error to connecter "+socket+" "+new NoReceiverAvailableException());
                    toWrite.put(CONNECTION_NO_ACCEPTOR);
                    toWrite.clear();
                  }

                  socket.write(toWrite);
                  if (toWrite.hasRemaining()) {
                    // this sucks, because the synchronization with the app-receiver becomes all wrong, this shouldn't normally happen
                    logger.warn("couldn't write 1 bite!!! {}", toWrite);
                    socket.close();
                    return;
                  }

                  if (success) {
                    acceptorAppl.finishReceiveSocket(new SocketAdapter<NodeHandle>(socket, node.getEnvironment()));
                  }
                } // sync
              } // if (acceptorAppl!=null)
            } // rSR()

            @Override
            public void receiveException(P2PSocket<NodeHandle> socket, Exception ioe) {
              logger.warn("incomingSocket({})", socket, ioe);
            }
          });
        }
      }

      @Override
      public void receiveException(
          P2PSocket<NodeHandle> socket,
          Exception ioe) {
        logger.warn("incomingSocket({})", socket, ioe);
      }

    });
  }
}
