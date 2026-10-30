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

import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.StatusConstants;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.IndexEntryFactory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactoryImpl;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryImpl;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.util.RandomAccessFileIOBuffer;

import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_RECV;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.EVT_SIGN;
import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_EXPOSED;

/**
 * Bob forks his log, that is, at some point he removes the k most
    recent entries and then continues recording.
    Expectation: Bob is exposed after the next audit (PROOF_CONSISTENCY)

 * @author Jeff Hoye
 *
 */
public class PRInconsistent1 extends PRRegressionTest {

  public PRInconsistent1() throws Exception {
    this(new File("."));
  }

  public PRInconsistent1(File dir) throws Exception {
    super(DEFAULT_MILLIS_TO_FINISH, dir);
  }

  @Override
  protected PeerReviewImpl<HandleImpl, IdImpl> getPeerReview(Player player, MyIdTL transport, Environment env) {
    if (player.localHandle.id.id == 2) {
      return new PeerReviewImpl<HandleImpl, IdImpl>(transport, env, new HandleSerializer(), new IdSerializer(), new IdExtractor(), getIdStrTranslator()
              ) {

        @Override
        protected SecureHistoryFactory getSecureHistoryFactory(IdentityTransport<HandleImpl, IdImpl> transport, final Environment env) {
          return new SecureHistoryFactoryImpl(transport, env) {

            @Override
            protected SecureHistoryImpl makeSecureHistory(RandomAccessFileIOBuffer indexFile, RandomAccessFileIOBuffer dataFile, boolean readOnly, HashProvider hashProv, IndexEntryFactory indexFactory, Logger logger) throws IOException {
              final ForkingSecureHistory foo = new ForkingSecureHistory(indexFile, dataFile, readOnly, hashProv, indexFactory, logger) {
                int acks = 0;

                @Override
                public void appendEntry(short type, boolean storeFullEntry,
                    ByteBuffer... entry) throws IOException {
                  super.appendEntry(type, storeFullEntry, entry);
                  // fork after the 3rd msg
                  if (type == EVT_SIGN) {
                    acks++;
                    if (acks == 3) {
                      final long idx = findLastEntry(new short[] {EVT_RECV}, Long.MAX_VALUE-10);
                      env.getSelectorManager().invoke(() -> {
                        try {
                          fork(idx);
                        } catch (IOException ioe) {
                          logger.warn("Error forking the history at index {}", idx, ioe);
                        }
                      });
                    }
                  }
                }
              };
              return foo;
            }
          };
        }
      };
    } else {
      return super.getPeerReview(player, transport, env);
    }
  }

  public class ForkingSecureHistory extends SecureHistoryImpl {

    public ForkingSecureHistory(RandomAccessFileIOBuffer indexFile,
        RandomAccessFileIOBuffer dataFile, boolean readOnly,
        HashProvider hashProv, IndexEntryFactory indexFactory, Logger logger)
        throws IOException {
      super(indexFile, dataFile, readOnly, hashProv, indexFactory, logger);
    }

    /**
     * Delete this index and everything after it
     */
    public void fork(long entryIndex) throws IOException {
      IndexEntry entry = statEntry(entryIndex);
      logger.info("forking at {} {}", entryIndex, entry);

      dataFile.setLength(entry.getFileIndex());
      indexFile.setLength(entryIndex*indexFactory.getSerializedSize());

      numEntries = entryIndex;
      topEntry = statEntry(entryIndex-1);
      assert(topEntry != null);
    }

  }

  @Override
  public void finish() {
    // see if everyone found bob exposed
    try {
      if (recordedStatus.get(aliceHandle).get(bobHandle.id) != StatusConstants.STATUS_EXPOSED) {
        logger.info("Alice Didn't expose bob");
        fail("Alice Didn't expose bob");
        return;
      }
      if (recordedStatus.get(carolHandle).get(bobHandle.id) != StatusConstants.STATUS_EXPOSED) {
        logger.info("Carol Didn't expose bob");
        fail("Carol Didn't expose bob");
        return;
      }
    } catch (Exception e) {
      // in case there is no record
      logger.warn("Failure", e);
      fail("No status recorded: "+e);
      return;
    }
    logger.info("Success");
    succeed();
  }

  @Override
  public BogusApp getBogusApp(Player player, final PeerReview<HandleImpl, IdImpl> pr,
      Environment env) {
    return new BogusApp(player,pr,env) {

      @Override
      public void notifyStatusChange(
          IdImpl id,
          int newStatus) {
        if (player.localHandle.id.id == 2) return; // ignore bob's stuff
        logger.atInfo().log(() -> "notifyStatusChange("+id+","+PeerReviewImpl.getStatusString(newStatus)+")");
        if (newStatus == STATUS_EXPOSED) {
          if (!id.equals(bobHandle.id)) {
            logger.info("Node not trusted: {}", id);
            fail("Node not trusted: "+id+" at "+player.localHandle);
            return;
          }
        }
        addStatusNotification(this.player.localHandle,id,newStatus);
      }

    };
  }

  public static void main(String[] args) throws Exception {
    runMain(new PRInconsistent1());
  }

}
