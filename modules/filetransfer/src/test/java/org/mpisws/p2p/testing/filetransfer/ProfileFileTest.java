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
package org.mpisws.p2p.testing.filetransfer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.mpisws.p2p.filetransfer.BBReceipt;
import org.mpisws.p2p.filetransfer.FileReceipt;
import org.mpisws.p2p.filetransfer.FileTransfer;
import org.mpisws.p2p.filetransfer.FileTransferCallback;
import org.mpisws.p2p.filetransfer.FileTransferImpl;
import org.mpisws.p2p.filetransfer.FileTransferListener;
import org.mpisws.p2p.filetransfer.Receipt;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.liveness.LivenessTransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessTransportLayerImpl;
import org.mpisws.p2p.transport.rc4.RC4TransportLayer;
import org.mpisws.p2p.transport.simpleidentity.InetSocketAddressSerializer;
import org.mpisws.p2p.transport.simpleidentity.SimpleIdentityTransportLayer;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.wire.WireTransportLayer;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;
import org.slf4j.Logger;
import rice.Continuation;
import rice.environment.Environment;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.pastry.transport.SocketAdapter;

/**
 *
 * @author Jeff Hoye
 *
 */
public class ProfileFileTest {

  public static void main(String[] args) throws Exception {
    // globals
    Path f = Path.of("delme.txt");
    if (!Files.exists(f)) {
      // make a 10 meg file
      Random r = new Random();
      try (OutputStream fos = Files.newOutputStream(f)) {
        byte[] foo = new byte[10000];
        for (int ctr = 0; ctr < 1000; ctr++) {
          r.nextBytes(foo);
          fos.write(foo);
        }
      }
    }

    final Environment env = new Environment();
    InetAddress local = InetAddress.getLocalHost();
    final Logger logger = env.getLogManager().getLogger(ProfileFileTest.class, null);
    DefaultErrorHandler<InetSocketAddress> errorHandler = new DefaultErrorHandler<InetSocketAddress>(logger);
    final TimeSource time = env.getTimeSource();
    logger.info("Encrypted Test");

    // this node will receive a file
    InetSocketAddress addr1 = new InetSocketAddress(local, 9001);
    WireTransportLayer wtl1 = new WireTransportLayerImpl(addr1,env,errorHandler);
    SimpleIdentityTransportLayer<InetSocketAddress, ByteBuffer> idtl1 = new SimpleIdentityTransportLayer<InetSocketAddress, ByteBuffer>(wtl1,new InetSocketAddressSerializer(), null ,env,errorHandler);
    LivenessTransportLayer<InetSocketAddress, ByteBuffer> ltl1 = new LivenessTransportLayerImpl<InetSocketAddress>(idtl1,env,errorHandler,300000);
    ltl1.addLivenessListener((i, val, options) -> logger.info("Node1: livenessChanged({},{},{})", i, val, options));

    RC4TransportLayer<InetSocketAddress, ByteBuffer> etl1 = new RC4TransportLayer<InetSocketAddress, ByteBuffer>(ltl1,env,"badpassword", errorHandler);

    etl1.setCallback(new TransportLayerCallback<InetSocketAddress, ByteBuffer>() {

      @Override
      public void messageReceived(InetSocketAddress i, ByteBuffer m,
          Map<String, Object> options) throws IOException {

      }

      @Override
      public void incomingSocket(P2PSocket<InetSocketAddress> s) throws IOException {
        // we got a socket, convert it to an AppSocket, then a FileTransfer
        logger.info("incomingSocket({})", s);
        final AppSocket sock = new SocketAdapter<InetSocketAddress>(s, env);
        FileTransfer ft = new FileTransferImpl(sock,new FileTransferCallback() {

          @Override
          public void messageReceived(ByteBuffer bb) {

          }

          @Override
          public void fileReceived(Path f, ByteBuffer metadata) {
            try {
              String name = new SimpleInputBuffer(metadata).readUTF();
              logger.info("file received {} named:{} size:{}", f, name, Files.size(f));
            } catch (IOException ioe) {
              logger.warn("Error interpreting filename ", ioe);
            }
          }

          @Override
          public void receiveException(Exception ioe) {
            logger.warn("Receiver FTC.receiveException()", ioe);
          }

        },env);



        ft.addListener(new FileTransferListener() {
          Map<FileReceipt, Long> startTimes = new HashMap<FileReceipt, Long>();

          @Override
          public void fileTransferred(FileReceipt receipt,
              long bytesTransferred, long total, boolean incoming) {
            synchronized(startTimes) {
              if (!startTimes.containsKey(receipt)) {
                startTimes.put(receipt, time.currentTimeMillis());
              }
            }
            if (bytesTransferred == total) {
              String s;
              if (incoming) {
                s = "download";
              } else {
                s = "upload";
              }
              long finTime = time.currentTimeMillis();
              long startTime = startTimes.get(receipt);
              double kbps = (1.0*total)/(1.0*(finTime-startTime));
              startTimes.remove(receipt);
              System.out.println("Took "+(finTime-startTime)+" millis to "+s+" "+total+" bytes. "+kbps+" kbps. Started at "+startTime);
            }
          }

          @Override
          public void msgTransferred(BBReceipt receipt, int bytesTransferred,
              int total, boolean incoming) {
            if (bytesTransferred == total) {
              String s;
              if (incoming) {
                s = "Downloaded";
              } else {
                s = "Uploaded";
              }
              System.out.println(s+" msg of size "+total);
            }
          }

          @Override
          public void transferCancelled(Receipt receipt, boolean incoming) {

          }

          @Override
          public void transferFailed(Receipt receipt, boolean incoming) {

          }

        });
      }

    });


    // this node will send a file
    InetSocketAddress addr2 = new InetSocketAddress(local, 9002);
    WireTransportLayer wtl2 = new WireTransportLayerImpl(addr2,env,errorHandler);
    SimpleIdentityTransportLayer<InetSocketAddress, ByteBuffer> idtl2 = new SimpleIdentityTransportLayer<InetSocketAddress, ByteBuffer>(wtl2,new InetSocketAddressSerializer(), null ,env,errorHandler);
    LivenessTransportLayer<InetSocketAddress, ByteBuffer> ltl2 = new LivenessTransportLayerImpl<InetSocketAddress>(idtl2,env,errorHandler,300000);

    // check liveness on addr1
    ltl2.addLivenessListener((i, val, options) -> logger.info("Node2: livenessChanged({},{},{})", i, val, options));
    ltl2.checkLiveness(addr1, null);

    RC4TransportLayer<InetSocketAddress, ByteBuffer> etl2 = new RC4TransportLayer<InetSocketAddress, ByteBuffer>(ltl2,env,"badpassword", errorHandler);

    etl2.openSocket(addr1, new SocketCallback<InetSocketAddress>() {

      @Override
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable,
          P2PSocket<InetSocketAddress> s) {
        logger.info("opened Socket {}", s);

        // we got the socket we requested, convert it to an AppSocket, then a FileTransfer
        final AppSocket sock = new SocketAdapter<InetSocketAddress>(s, env);
        FileTransfer ft = new FileTransferImpl(sock, new FileTransferCallback() {

          @Override
          public void messageReceived(ByteBuffer bb) {

          }

          @Override
          public void fileReceived(Path f, ByteBuffer metadata) {

          }

          @Override
          public void receiveException(Exception ioe) {
            logger.warn("Sender FTC.receiveException()", ioe);
          }
        }, env);

        // send a file normal priority, don't worry about notification of completion
        try {
          SimpleOutputBuffer sob = new SimpleOutputBuffer();
          sob.writeUTF("foo");
          ft.sendFile(Path.of("delme.txt"), sob.getByteBuffer(), (byte)0, new Continuation<FileReceipt, Exception>() {

            @Override
            public void receiveResult(FileReceipt result) {
              System.out.println("Send success "+result);
            }

            @Override
            public void receiveException(Exception exception) {
              System.out.println("Send Failed");
            }

          });
        } catch (IOException ioe) {
          logger.warn("Error sending file.", ioe);
        }

      }

      @Override
      public void receiveException(SocketRequestHandle<InetSocketAddress> s,
          Exception ex) {
        logger.warn("receiveException({})", s, ex);
      }
    }, null);


  }

}
