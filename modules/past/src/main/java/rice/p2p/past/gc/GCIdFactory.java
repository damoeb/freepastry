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

package rice.p2p.past.gc;

import java.util.Random;
import java.util.SortedMap;

import rice.environment.random.RandomSource;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.NodeHandleSet;

/**
 * This class provides the ability to build Ids which can support a multi-ring
 * hierarchy.
 *
 * @version $Id: GCIdFactory.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Peter Druschel
 */
public class GCIdFactory implements IdFactory {

  /**
   * The underlying IdFactory
   */
  private final IdFactory factory;

  /**
   * Constructor
   *
   * @param factory the underlying factory to use
   */
  public GCIdFactory(IdFactory factory) {
    this.factory = factory;
  }

  /**
   * Not supported.
   *
   * @param material The material to use
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  public Id buildNormalId(byte[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Not supported.
   *
   * @param material The material to use
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Id buildId(byte[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Not supported.
   *
   * @param material The material to use
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Id buildId(int[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Not supported.
   *
   * @param string The string to use as source data
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Id buildId(String string) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Not supported.
   *
   * @param rng A random number generator
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Id buildRandomId(Random rng) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  @Override
  public Id buildRandomId(RandomSource rng) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Not supported.
   *
   * @param string The toString() representation of an Id
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Id buildIdFromString(String string) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Not supported.
   *
   * @param chars The character array
   * @param offset The offset to start reading at
   * @param length The length to read
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Id buildIdFromString(char[] chars, int offset, int length) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Not supported.
   *
   * @param string The toString() representation of an Id
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public IdRange buildIdRangeFromPrefix(String string) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Not supported.
   *
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public int getIdStringLength() {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Not supported.
   *
   * @param material The material to use
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Id.Distance buildIdDistance(byte[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build IdDistances!");  }

  /**
   * Creates an IdRange given the CCW and CW ids.
   *
   * @param ccw The counterclockwise Id, the inclusive start of the range
   * @param cw The clockwise Id, the exclusive end of the range
   * @return An IdRange with the appropriate delimiters.
   */
  @Override
  public IdRange buildIdRange(Id ccw, Id cw) {
    return new GCIdRange(factory.buildIdRange(ccw, cw));
  }

  /**
   * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  @Override
  public IdSet buildIdSet() {
    return new GCIdSet(factory);
  }

  /**
   * Not supported.
   *
   * @param map ignored
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public IdSet buildIdSet(SortedMap<Id, ?> map) {
    throw new UnsupportedOperationException("GCIdFactory.buildIDSet()");
  }

  /**
   * Not supported.
   *
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public NodeHandleSet buildNodeHandleSet() {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build NodeHandleSets!");
  }
}
