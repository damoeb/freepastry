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
package rice.p2p.glacier;
import java.util.Random;
import java.util.SortedMap;
import java.util.StringTokenizer;

import rice.environment.random.RandomSource;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.multiring.MultiringIdFactory;
import rice.p2p.multiring.RingId;

/**
 * Builds FragmentKeys, and ranges and sets of them, on top of a MultiringIdFactory.
 *
 * @version $Id: FragmentKeyFactory.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author ahae
 */
public class FragmentKeyFactory extends GlacierKeyFactory {

  /**
   * Constructor for FragmentKeyFactory.
   *
   * @param factory the factory used to build the underlying ids
   */
  public FragmentKeyFactory(MultiringIdFactory factory) {
    super(factory);
  }

  /**
   * Builds a random protocol-specific Id.
   *
   * @param rng A random number generator
   * @return The built Id.
   */
  @Override
  public Id buildRandomId(Random rng) {
    return new FragmentKey(new VersionKey(factory.buildRandomId(rng), rng.nextLong()), rng.nextInt());
  }

  @Override
  public Id buildRandomId(RandomSource rng) {
    return new FragmentKey(new VersionKey(factory.buildRandomId(rng), rng.nextLong()), rng.nextInt());
  }

  /**
   * Builds a key from its toString() form
   *
   * @param string the string form
   * @return the key
   */
  @Override
  public Id buildIdFromToString(String string) {
    StringTokenizer stok = new StringTokenizer(string, "(,) :v#");
    if (stok.countTokens() < 4) {
      return null;
    }

    String keyRingS = stok.nextToken();
    String keyNodeS = stok.nextToken();
    String versionS = stok.nextToken();
    String fragmentIdS = stok.nextToken();
    RingId key = factory.buildRingId(rice.pastry.Id.build(keyRingS), rice.pastry.Id.build(keyNodeS));

    return new FragmentKey(new VersionKey(key, Long.valueOf(versionS).longValue()), Integer.valueOf(fragmentIdS).intValue());
  }

  /**
   * Builds an IdRange based on a prefix.  Any id which has this prefix should
   * be inside this IdRange, and any id which does not share this prefix should
   * be outside it.
   *
   * @param string The toString() representation of an Id
   * @return The built Id.
   */
  @Override
  public IdRange buildIdRangeFromPrefix(String string) {
    return new FragmentKeyRange(factory.buildIdRangeFromPrefix(string));
  }

  /**
   * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  @Override
  public IdSet buildIdSet() {
    return new FragmentKeySet();
  }

  /**
   * Creates an IdSet whose members are the keys of the given map.
   *
   * @param map The map which to take the keys from to create the IdSet's elements
   * @return the IdSet
   */
  @Override
  public IdSet buildIdSet(SortedMap<Id, ?> map) {
    return new FragmentKeySet(map);
  }

  @Override
  public int getIdToStringLength() {
    return factory.getIdToStringLength() + 4;
  }
}
