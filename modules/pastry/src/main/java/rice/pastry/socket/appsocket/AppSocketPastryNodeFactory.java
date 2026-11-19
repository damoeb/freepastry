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
package rice.pastry.socket.appsocket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

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
import org.mpisws.p2p.transport.identity.IdentityImpl;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.OverrideLiveness;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.wire.SocketManager;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.MathUtils;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.socket.SocketNodeHandleFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.socket.TransportLayerNodeHandle;
import rice.pastry.transport.NodeHandleAdapter;
import rice.pastry.transport.SocketAdapter;
import rice.pastry.transport.TransportLayerDeserializer;


/**
 * Extends SocketPastryNodeFactory and adds getSocketFactory() to allow access to
 * a FreePastry application w/o joining the Ring.
 *
 * @author Jeff Hoye
 *
 */
public class AppSocketPastryNodeFactory extends SocketPastryNodeFactory {

  /**
   * Used for getSocketChannel.  The AppSocketFactroyLayer stores the channel here,
   * and then it is retrieved when the channel is opened.
   */
  private final Map<Integer, SocketManager> socketTable = new HashMap<Integer, SocketManager>();

  public AppSocketPastryNodeFactory(NodeIdFactory nf, int startPort,
      Environment env) throws IOException {
    super(nf, startPort, env);
  }

  public static final String STORE_SOCKET = "store_socket";

  class AppSocketFactoryLayer implements
    TransportLayer<InetSocketAddress, ByteBuffer>,
    TransportLayerCallback<InetSocketAddress, ByteBuffer> {

    private final WireTransportLayer wtl;
    /**
     * Indexed by the option "STORE_SOCKET"
     */
    private TransportLayerCallback<InetSocketAddress, ByteBuffer> callback;

    public AppSocketFactoryLayer(WireTransportLayer wtl) {
      this.wtl = wtl;
      this.wtl.setCallback(this);
    }

    @Override
    public void acceptMessages(boolean b) {
      wtl.acceptMessages(b);
    }

    @Override
    public void acceptSockets(boolean b) {
      wtl.acceptSockets(b);
    }

    @Override
    public InetSocketAddress getLocalIdentifier() {
      return wtl.getLocalIdentifier();
    }

    @Override
    public SocketRequestHandle<InetSocketAddress> openSocket(
        final InetSocketAddress i,
        final SocketCallback<InetSocketAddress> deliverSocketToMe,
        final Map<String, Object> options) {

      return wtl.openSocket(i, new SocketCallback<InetSocketAddress>() {

        @Override
        public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable,
            P2PSocket<InetSocketAddress> sock) {

          SocketManager sa = (SocketManager)sock;
          if (options.containsKey(STORE_SOCKET)) {
            socketTable.put((Integer)options.get(STORE_SOCKET), sa);
          }
          deliverSocketToMe.receiveResult(cancellable, sock);
        }

        @Override
        public void receiveException(SocketRequestHandle<InetSocketAddress> s,
            Exception ex) {
          deliverSocketToMe.receiveException(s,ex);
        }

      }, options);
    }

    @Override
    public MessageRequestHandle<InetSocketAddress, ByteBuffer> sendMessage(
        InetSocketAddress i, ByteBuffer m,
        MessageCallback<InetSocketAddress, ByteBuffer> deliverAckToMe,
        Map<String, Object> options) {
      return wtl.sendMessage(i, m, deliverAckToMe, options);
    }

    @Override
    public void setCallback(
        TransportLayerCallback<InetSocketAddress, ByteBuffer> callback) {
      this.callback = callback;
    }

    @Override
    public void setErrorHandler(ErrorHandler<InetSocketAddress> handler) {

    }

    @Override
    public void incomingSocket(P2PSocket<InetSocketAddress> s)
        throws IOException {
      callback.incomingSocket(s);
    }

    @Override
    public void messageReceived(InetSocketAddress i, ByteBuffer m,
        Map<String, Object> options) throws IOException {
      callback.messageReceived(i, m, options);
    }

    @Override
    public void destroy() {

    }
  }

  class BogusTLPastryNode extends PastryNode  {


    public BogusTLPastryNode(Id id, Environment e) {
      super(id, e);
    }

    public TransportLayer<InetSocketAddress, ByteBuffer> getWireTransportLayer(
        WireTransportLayer wtl) {
      return new AppSocketFactoryLayer(wtl);
    }

    public void setLocalHandle(NodeHandle localhandle) {
      this.localhandle = localhandle;
    }

  }

  SocketFactory sf;

  /**
   * @return used to open sockets to the remote node
   */
  public synchronized SocketFactory getSocketFactory() throws IOException {
    if (sf != null) return sf;

    // build a bogus PastryNode for this
    Id nodeId = Id.build();
    final BogusTLPastryNode pn = new BogusTLPastryNode(nodeId, environment);

    final SocketNodeHandleFactory handleFactory = (SocketNodeHandleFactory)getNodeHandleFactory(pn);
    final NodeHandle localhandle = getLocalHandle(pn, handleFactory);

    pn.setLocalHandle(localhandle);

    TransportLayerDeserializer deserializer = getTransportLayerDeserializer(handleFactory,pn);

    final NodeHandleAdapter nha = getNodeHandleAdapter(pn, handleFactory, deserializer);

    sf = new SocketFactory() {
      int uid = Integer.MIN_VALUE;
      @Override
      public Cancellable getAppSocket(InetSocketAddress addr, int appid, final Continuation<AppSocket, Exception> c, Map<String, Object> options) {
        return getSocket(addr, appid,
            new Continuation<P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>>, Exception>(){

              @Override
              public void receiveException(Exception exception) {
                c.receiveException(exception);
              }

              @Override
              public void receiveResult(
                  P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> result) {
                c.receiveResult(new SocketAdapter<TransportLayerNodeHandle<MultiInetSocketAddress>>(result, environment));
              }
            }, options);
      }

      @Override
      public Cancellable getSocketChannel(InetSocketAddress addr, int appid, final Continuation<SocketChannel, Exception> c, Map<String, Object> options) {
        // increment the uid
        final int myUid;
        synchronized(this) {
          myUid = uid++;
        }
        // add a UID in the options
        options = OptionsFactory.addOption(options, STORE_SOCKET, myUid);

        // open the socket
        // the AppSocketFactoryLayer will intercept the SocketManager from the wtl using the UID

        return getSocket(addr, appid,
            new Continuation<P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>>, Exception>(){

              @Override
              public void receiveException(Exception exception) {
                c.receiveException(exception);
              }

              @Override
              public void receiveResult(
                  P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> result) {
                // return the socket
                SocketManager sm = socketTable.remove(myUid);
                SocketChannel ret = sm.getSocketChannel();
                c.receiveResult(ret);
              }
            }, options);

      }


      Cancellable getSocket(InetSocketAddress addr, final int appid, final Continuation<P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>>, Exception> c, Map<String, Object> options) {
        TransportLayerNodeHandle<MultiInetSocketAddress> handle = getHandle(addr);
        final SocketRequestHandleImpl<TransportLayerNodeHandle<MultiInetSocketAddress>> ret =
          new SocketRequestHandleImpl<TransportLayerNodeHandle<MultiInetSocketAddress>>(handle, options, logger);

        options = OptionsFactory.addOption(options, IdentityImpl.DONT_VERIFY, true);
        // open the socket
        // send the id
        ret.setSubCancellable(getTL().openSocket(handle, new SocketCallback<TransportLayerNodeHandle<MultiInetSocketAddress>>() {

          @Override
          public void receiveResult(
              SocketRequestHandle<TransportLayerNodeHandle<MultiInetSocketAddress>> cancellable,
              final P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> sock) {
            ret.setSubCancellable(() -> {
              sock.close();
              return true;
            });

            try {
              new P2PSocketReceiver<TransportLayerNodeHandle<MultiInetSocketAddress>>() {
                ByteBuffer buf;
                {
                  byte[] idBytes = MathUtils.intToByteArray(appid);
                  buf = ByteBuffer.wrap(idBytes);
                }

                @Override
                public void receiveSelectResult(
                    P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> socket,
                    boolean canRead, boolean canWrite) throws IOException {
                  // send the id
                  if (socket.write(buf) < 0) {
                    c.receiveException(new ClosedChannelException("Socket was closed by remote host. "+socket));
                    return;
                  }
                  if (buf.hasRemaining()) {
                    socket.register(false, true, this);
                    return;
                  }
                  c.receiveResult(socket);
                }
                @Override
                public void receiveException(
                    P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> socket,
                    Exception ioe) {
                  c.receiveException(ioe);
                }
              }.receiveSelectResult(sock, false, true);
            } catch (IOException ioe) {
              c.receiveException(ioe);
            }
          }

          @Override
          public void receiveException(
              SocketRequestHandle<TransportLayerNodeHandle<MultiInetSocketAddress>> s,
              Exception ex) {

          }

        }, options));
        return ret;
      }

      TransportLayerNodeHandle<MultiInetSocketAddress> getHandle(InetSocketAddress addr) {
        return handleFactory.getNodeHandle(new MultiInetSocketAddress(addr), -1, Id.build());
      }

      TransportLayer<TransportLayerNodeHandle<MultiInetSocketAddress>, RawMessage> getTL() {
        return (TransportLayer<TransportLayerNodeHandle<MultiInetSocketAddress>, RawMessage>)nha.getTransportLayer();
      }

    };
    return sf;
  }

  /**
   * This code tells the WireTransportLayer not to create a server-socket if we are using the BogusTLPastryNode
   */
  @Override
  protected TransportLayer<InetSocketAddress, ByteBuffer> buildWireTransportLayer(InetSocketAddress innermostAddress, final PastryNode pn) throws IOException {
    Environment environment = pn.getEnvironment();
    WireTransportLayer wtl = new WireTransportLayer(innermostAddress,environment, null, !(pn instanceof BogusTLPastryNode));

    if (pn instanceof BogusTLPastryNode bogusTlPastryNode) {
      return bogusTlPastryNode.getWireTransportLayer(wtl);
    }

    return wtl;
  }

  /**
   * If it's for a remote node, eliminate the liveness layer, and just return bogus results
   */
  @Override
  protected TransLiveness<SourceRoute<MultiInetSocketAddress>, ByteBuffer> getLivenessTransportLayer(
      final TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> tl,
      PastryNode pn) {
    if (pn instanceof BogusTLPastryNode) {
      final NoOpLiveness<SourceRoute<MultiInetSocketAddress>> noOp = new NoOpLiveness<>();
      return new TransLiveness<SourceRoute<MultiInetSocketAddress>, ByteBuffer>(){
        @Override
        public TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> getTransportLayer() {
          return tl;
        }
        @Override
        public LivenessProvider<SourceRoute<MultiInetSocketAddress>> getLivenessProvider() {
          return noOp;
        }
        @Override
        public OverrideLiveness<SourceRoute<MultiInetSocketAddress>> getOverrideLiveness() {
          return noOp;
        }
        @Override
        public Pinger<SourceRoute<MultiInetSocketAddress>> getPinger() {
          return noOp;
        }
      };
    }

    return super.getLivenessTransportLayer(tl, pn);
  }
}
