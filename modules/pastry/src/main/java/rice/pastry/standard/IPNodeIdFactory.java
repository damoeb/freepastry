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
package rice.pastry.standard;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.util.HashUtils;
import rice.pastry.Id;
import rice.pastry.NodeIdFactory;

/**
 * Constructs NodeIds for virtual nodes derived from the IP address and port
 * number of this Java VM.
 *
 * @version $Id: IPNodeIdFactory.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Peter Druschel
 */

public class IPNodeIdFactory implements NodeIdFactory {
  /**
   * Deliberately JVM-wide, so that all factories in this JVM that share an IP and port still generate distinct ids.
   */
  private static final AtomicInteger nextInstance = new AtomicInteger();

  private InetAddress localIP;

  private int port;


  private final Logger logger;

  /**
   * Constructor.
   *
   * @param port the port number on which this Java VM listens
   */

  public IPNodeIdFactory(InetAddress localIP, int port, Environment env) {
    this.port = port;
    this.logger = env.getLogManager().getLogger(getClass(), null);
    try {
      this.localIP = localIP;
      if (localIP.isLoopbackAddress())
        throw new Exception(
            "got loopback address: nodeIds will not be unique across computers!");
    } catch (Exception e) {
      logger.atError().log(() -> "ALERT: IPNodeIdFactory cannot determine local IP address: "+ e);
    }
  }

  /**
   * generate a nodeId multiple invocations result in a deterministic series of
   * randomized NodeIds, seeded by the IP address of the local host.
   *
   * @return the new nodeId
   */

  @Override
  public Id generateNodeId() {
    byte rawIP[] = localIP.getAddress();

    byte rawPort[] = new byte[2];
    int tmp = port;
    for (int i = 0; i < 2; i++) {
      rawPort[i] = (byte) (tmp & 0xff);
      tmp >>= 8;
    }

    byte bytes[] = new byte[4];
    tmp = nextInstance.incrementAndGet();
    for (int i = 0; i < 4; i++) {
      bytes[i] = (byte) (tmp & 0xff);
      tmp >>= 8;
    }

    Id nodeId = Id.build(HashUtils.hashToIdMaterial(rawIP, rawPort, bytes));

    return nodeId;
  }

}
