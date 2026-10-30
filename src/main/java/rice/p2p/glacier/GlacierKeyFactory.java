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

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.multiring.MultiringIdFactory;

/**
 * Builds Glacier keys, and ranges and sets of them, on top of a MultiringIdFactory. Keys cannot be built from raw
 * material, and there are no distances, explicit ranges or node handle sets of keys.
 *
 * @author ahae
 */
public abstract class GlacierKeyFactory implements IdFactory {

  /**
   * The factory of the ids the keys are made of
   */
  protected final MultiringIdFactory factory;

  /**
   * Constructor
   *
   * @param factory the factory used to build the underlying ids
   */
  protected GlacierKeyFactory(MultiringIdFactory factory) {
    this.factory = factory;
  }

  private RuntimeException unsupported(String method) {
    return new RuntimeException(getClass().getSimpleName() + "." + method + " is not supported!");
  }

  /**
   * Not supported.
   *
   * @param material the raw id material
   * @return never returns
   * @throws RuntimeException always
   */
  public Id buildId(byte[] material) {
    throw unsupported("buildId(byte[])");
  }

  /**
   * Not supported.
   *
   * @param material The material to use
   * @return never returns
   * @throws RuntimeException always
   */
  public Id buildId(int[] material) {
    throw unsupported("buildId(int[])");
  }

  /**
   * Not supported.
   *
   * @param string The string to use as source data
   * @return never returns
   * @throws RuntimeException always
   */
  public Id buildId(String string) {
    throw unsupported("buildId(String)");
  }

  public Id buildIdFromToString(char[] chars, int offset, int length) {
    return buildIdFromToString(new String(chars, offset, length));
  }

  /**
   * Not supported.
   *
   * @param material The material to use
   * @return never returns
   * @throws RuntimeException always
   */
  public Id.Distance buildIdDistance(byte[] material) {
    throw unsupported("buildIdDistance()");
  }

  /**
   * Not supported.
   *
   * @param cw The clockwise Id
   * @param ccw The counterclockwise Id
   * @return never returns
   * @throws RuntimeException always
   */
  public IdRange buildIdRange(Id cw, Id ccw) {
    throw unsupported("buildIdRange()");
  }

  /**
   * Not supported.
   *
   * @return never returns
   * @throws RuntimeException always
   */
  public NodeHandleSet buildNodeHandleSet() {
    throw unsupported("buildNodeHandleSet()");
  }
}
