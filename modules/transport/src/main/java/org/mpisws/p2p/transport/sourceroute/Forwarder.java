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
package org.mpisws.p2p.transport.sourceroute;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.slf4j.Logger;


public class Forwarder<Identifier> {
  /** Bytes copied per read while forwarding a socket. */
  private static final int PIPE_BUFFER_SIZE = 1024;

  SourceRoute<Identifier> sr;
  P2PSocket<Identifier> socka;
  P2PSocket<Identifier> sockb;
  Logger logger;
  /**
   * Notified of the forwarded bytes and when the forwarding ends.
   */
  Collection<SourceRouteTap<Identifier>> taps;
  /**
   * Number of directions whose input has been shut down (0..2).
   */
  int numShutdown = 0;
  boolean closed = false;

  public Forwarder(SourceRoute<Identifier> sr, P2PSocket<Identifier> socka, P2PSocket<Identifier> sockb, Logger logger) {
    this(sr, socka, sockb, logger, Collections.<SourceRouteTap<Identifier>>emptyList());
  }

  /**
   * @param taps notified via {@link SourceRouteTap#receivedBytes} and {@link SourceRouteTap#socketClosed};
   * the collection is read live, so taps added/removed later are honored
   */
  public Forwarder(SourceRoute<Identifier> sr, P2PSocket<Identifier> socka, P2PSocket<Identifier> sockb, Logger logger, Collection<SourceRouteTap<Identifier>> taps) {
    this.sr = sr;
    this.socka = socka;
    this.sockb = sockb;
    this.logger = logger;
    this.taps = taps;

    new HalfPipe(socka,sockb);
    new HalfPipe(sockb,socka);
  }

  /**
   * Called when one direction has been fully forwarded and its output shut down.  When both are done, close both sockets.
   */
  private void halfPipeShutdown() {
    synchronized(this) {
      numShutdown++;
      if (numShutdown < 2) return;
    }
    close();
  }

  private void close() {
    synchronized(this) {
      if (closed) return;
      closed = true;
    }
    socka.close();
    sockb.close();
    for (SourceRouteTap<Identifier> tap : taps) {
      tap.socketClosed(sr, socka, sockb);
    }
  }

  private class HalfPipe implements P2PSocketReceiver<Identifier> {
    P2PSocket<Identifier> from;
    P2PSocket<Identifier> to;
    ByteBuffer buf;

    HalfPipe(P2PSocket<Identifier> from, P2PSocket<Identifier> to) {
      this.from = from;
      this.to = to;
      buf = ByteBuffer.allocate(PIPE_BUFFER_SIZE);
      from.register(true, false, this);
    }

    @Override
    public String toString() {
      return "HalfPipe "+from+"=>"+to;
    }

    @Override
    public void receiveException(P2PSocket<Identifier> socket, Exception e) {
      logger.debug("{} {}", this, socket, e);
      close();
    }

    @Override
    @SuppressWarnings("ReferenceEquality") // the same instance is meant
    public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
      if (canRead) {
        if (socket != from) throw new IOException("Expected to read from "+from+" got "+socket);
        long result = from.read(buf);
        if (result == -1) {
          logger.debug("{} has shut down input, shutting down output on {}", from, to);
          // nothing is buffered (we only read after buf has been fully written), so shut down now
          to.shutdownOutput();
          halfPipeShutdown();
          return;
        }
        logger.trace("Read {} bytes from {}", result, from);
        buf.flip();
        if (!taps.isEmpty()) {
          for (SourceRouteTap<Identifier> tap : taps) {
            tap.receivedBytes(ByteBuffer.wrap(Arrays.copyOfRange(buf.array(), buf.position(), buf.limit())), sr, from, to);
          }
        }
        to.register(false, true, this);
      } else {
        if (canWrite) {
          if (socket != to) throw new IOException("Expected to write to "+to+" got "+socket);

          long result = to.write(buf);
          if (result == -1) {
            logger.debug("{} has closed, closing {}", to, from);
            close();
            return;
          }
          logger.trace("Wrote {} bytes to {}", result, to);


          if (buf.hasRemaining()) {
            // keep writing
            to.register(false, true, this);
          } else {
            // read again
            buf.clear();
            from.register(true, false, this);
          }
        } else {
          throw new IOException("Didn't select for either "+socket);
        }
      }
    }
  }
}
