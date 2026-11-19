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

import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.StatusConstants;
import rice.environment.Environment;

import static org.mpisws.p2p.transport.peerreview.StatusConstants.STATUS_EXPOSED;

/**
 * Alice deviates from the protocol by sending a message that's different
 * from the one she is supposed to send.
    Expectation: After the first audit, Alice is exposed by everyone.
    (PROOF_CONFORMANCE)
 * @author Jeff Hoye
 *
 */
public class PRNonconform2 extends PRRegressionTest {

  public PRNonconform2() throws Exception {
    this(new File("."));
  }

  public PRNonconform2(File dir) throws Exception {
    super(DEFAULT_MILLIS_TO_FINISH, dir);
  }

  @Override
  public void finish() {
    // see if everyone found bob exposed
    try {
      if (recordedStatus.get(bobHandle).get(aliceHandle.id) != StatusConstants.STATUS_EXPOSED) {
        logger.info("Bob Didn't expose Alice");
        fail("Bob Didn't expose Alice");
        return;
      }
      if (recordedStatus.get(carolHandle).get(aliceHandle.id) != StatusConstants.STATUS_EXPOSED) {
        logger.info("Carol Didn't expose Alice");
        fail("Carol Didn't expose Alice");
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
  public BogusApp getBogusApp(Player player, PeerReview<HandleImpl, IdImpl> peerReview,
      Environment env) {
    if (player.localHandle.id.id == 1) {
      // alice
      return new BogusApp(player,peerReview,env) {
        int msgNum = 0;

        /**
         * On the 3rd message, modify one of the bits.
         */
        @Override
        protected byte[] generateMessage() {
          byte[] ret = super.generateMessage();
          msgNum++;
          if (msgNum == 3) {
            ret[ret.length-1]++;
          }
          return ret;
        }

        @Override
        public void notifyStatusChange(
            IdImpl id,
            int newStatus) {
          // ignore alice's stuff
        }

      };
    } else {
      return new BogusApp(player,peerReview,env) {
       @Override
        public void notifyStatusChange(
            IdImpl id,
            int newStatus) {
          if (player.localHandle.id.id == 1) return; // ignore alice's stuff
          logger.atInfo().log(() -> "notifyStatusChange("+id+","+PeerReviewImpl.getStatusString(newStatus)+")");
          if (newStatus == STATUS_EXPOSED) {
            if (!id.equals(aliceHandle.id)) {
              logger.info("Node not trusted: {}", id);
              fail("Node not trusted: "+id+" at "+player.localHandle);
              return;
            }
          }
          addStatusNotification(this.player.localHandle,id,newStatus);
        }

      };
    }
  }

  public static void main(String[] args) throws Exception {
    runMain(new PRNonconform2());
  }

}
