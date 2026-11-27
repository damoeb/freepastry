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
package org.mpisws.p2p.transport.peerreview.replay;

import java.io.IOException;

import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.replay.playback.ReplaySelectorManager;
import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.logging.Slf4jLogManager;
import rice.environment.params.Parameters;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.processing.Processor;
import rice.environment.processing.sim.SimProcessor;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.selector.SelectorLoop;

public class VerifierFactory<Handle extends RawSerializable, Identifier extends RawSerializable> {

  PeerReview<Handle, Identifier> peerreview;

  public VerifierFactory(PeerReview<Handle, Identifier> peerreview) {
    this.peerreview = peerreview;
  }

  public Verifier<Handle, Identifier> getVerifier(SecureHistory history,
      Handle localHandle, long firstEntryToReplay, long initialTime,
      Object extInfo) throws IOException {
    Environment env = generateEnvironment(localHandle.toString(), 0, 0,peerreview.getEnvironment().getLogManager());
    Verifier<Handle, Identifier> ret = new Verifier<Handle, Identifier>(peerreview,env,history,localHandle,firstEntryToReplay,extInfo);
    ((ReplaySelectorManager)env.getSelectorManager()).setVerifier(ret);
    return ret;
  }

  public static Environment generateEnvironment(String name, long startTime, long randSeed, LogManager lm2) {
    Parameters params = new SimpleParameters(Environment.defaultParamFileArray,null);
    DirectTimeSource dts = new DirectTimeSource(startTime);

    LogManager logManager;
    if (lm2 instanceof Slf4jLogManager clm) {
      logManager = clm.clone(clm.getPrefix()+"-"+name);
    } else {
      logManager = Environment.generateDefaultLogManager();
    }
    RandomSource randomSource = new SimpleRandomSource(randSeed, logManager);
    dts.setLogManager(logManager);
    SelectorLoop selector = new ReplaySelectorManager("Replay "+name, dts, logManager);
    dts.setSelectorManager(selector);
    Processor proc = new SimProcessor(selector);
    return new Environment(selector,proc,randomSource,dts,logManager,
        params, Environment.generateDefaultExceptionStrategy(logManager));
  }

}
