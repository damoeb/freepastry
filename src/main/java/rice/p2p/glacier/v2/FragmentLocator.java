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
package rice.p2p.glacier.v2;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.glacier.FragmentKey;

/**
 * Places fragments on the ring: fragment {@code i} of version {@code v} of an object lies at a fixed
 * fraction of the ring away from the object key.
 */
final class FragmentLocator {
  private final IdFactory factory;
  private final int numFragments;

  FragmentLocator(IdFactory factory, int numFragments) {
    this.factory = factory;
    this.numFragments = numFragments;
  }

  private static byte[] getDistance(double d) {
    byte[] result = new byte[20];

    double c = 0.5;
    for (int i=19; i>=0; i--) {
      result[i] = 0;
      for (int j=7; j>=0; j--) {
        if (d >= c) {
          result[i] |= (1<<j);
          d -= c;
        }
        c /= 2;
      }
    }

    return result;
  }

  /**
   * Determines the point in the ring where a particular fragment should
   * be stored.
   *
   * @param objectKey Key of the original object (from PAST)
   * @param fragmentNr Fragment number (0..n-1)
   * @param version Version of the object
   * @return The location of the fragment
   */
  Id getFragmentLocation(Id objectKey, int fragmentNr, long version) {
    double totalOffset = (((float)fragmentNr) / ((float)numFragments)) + version * (1.0/2.7182821);
    return objectKey.addToId(factory.buildIdDistance(getDistance(totalOffset - Math.floor(totalOffset))));
  }

  Id getFragmentLocation(FragmentKey fkey) {
    return getFragmentLocation(
      fkey.getVersionKey().getId(),
      fkey.getFragmentID(),
      fkey.getVersionKey().getVersion()
    );
  }
}
