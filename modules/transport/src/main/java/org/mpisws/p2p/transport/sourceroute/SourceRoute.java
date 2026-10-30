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
import java.util.ArrayList;
import java.util.List;

import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * Class which represents a source route to a remote identifier: the list of hops from the source to the destination.
 *
 * @author Alan Mislove
 */
public abstract class SourceRoute<Identifier> {

  // the hops of this route, from the source to the destination
  protected List<Identifier> path;

  /**
   * Constructor
   *
   * @param path the hops of the route, none of them null
   */
  protected SourceRoute(List<Identifier> path) {
    this.path = new ArrayList<Identifier>(path);

    // assertion
    for (Identifier i : this.path) {
      if (i == null) throw new IllegalArgumentException("path["+i+"] is null");
    }
  }

  protected SourceRoute(Identifier address) {
    this.path = new ArrayList<Identifier>(1);
    path.add(address);
  }

  protected SourceRoute(Identifier local, Identifier remote) {
    this.path = new ArrayList<Identifier>(2);
    path.add(local);
    path.add(remote);
  }

  /**
   * Returns the hashCode of this source route
   *
   * @return The hashCode
   */
  @Override
  public int hashCode() {
    int result = 399388937;

    for (Identifier i : path)
      result ^= i.hashCode();

    return result;
  }

  /**
   * Checks equality on source routes
   *
   * @param o The source route to compare to
   * @return The equality
   */
  @Override
  public boolean equals(Object o) {
    if (o == null)
      return false;

    if (!(o instanceof SourceRoute))
      return false;

    SourceRoute<?> that = (SourceRoute<?>)o;

    return this.path.equals(that.path);
  }

  /**
   * Returns the hops of this route, separated by arrows
   *
   * @return The string
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("{");

    for (int i=0; i<path.size(); i++) {
      Identifier thePath = path.get(i);
      result.append(thePath.toString());
      if (i < path.size() - 1) result.append(" -> ");
    }

    result.append("}");

    return result.toString();
  }

  /**
   * Method which returns the first "hop" of this source route
   *
   * @return The first hop of this source route
   */
  public Identifier getFirstHop() {
    return path.get(0);
  }

  /**
   * Method which returns the first "hop" of this source route
   *
   * @return The first hop of this source route
   */
  public Identifier getLastHop() {
    return path.get(path.size()-1);
  }

  /**
   * Returns the number of hops in this source route
   *
   * @return The number of hops
   */
  public int getNumHops() {
    return path.size();
  }

  /**
   * Returns the hop at the given index
   *
   * @param i The hop index
   * @return The hop
   */
  public Identifier getHop(int i) {
    return path.get(i);
  }

  /**
   * Returns whether or not this route is direct
   *
   * @return whether or not this route is direct
   */
  public boolean isDirect() {
    return (path.size() <= 2);
  }

  /**
   * Returns which hop in the path the identifier is.
   * @param identifier the hop
   * @return -1 if not in the path
   */
  public int getHop(Identifier identifier) {
    for(int i = 0; i < path.size(); i++) {
      Identifier id = path.get(i);
      if (id.equals(identifier)) return i;
    }
    return -1;
  }


  /***************** Raw Serialization ***************************************/
  abstract public void serialize(OutputBuffer buf) throws IOException;
  abstract public int getSerializedLength();



}
