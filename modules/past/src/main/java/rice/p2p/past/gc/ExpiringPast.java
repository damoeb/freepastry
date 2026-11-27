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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import rice.Outcome;
import rice.p2p.commonapi.Id;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;

/**
 * This interface represents an extension to the Past interface, which adds
 * support for garbage collection and versionsing.  Individual objects are
 * each given versions and timeout times.  Version numbers are used to keep
 * different replicas in sync, and the timeout times are used to determine
 * when space can be reclaimed.
 *
 * Applications must periodically invoke the refresh() operation on all objects
 * which they are interested in.  Otherwise, objects may be reclaimed and
 * will be no longer available.
 *
 * @version $Id: ExpiringPast.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Andreas Haeberlen
 */
public interface ExpiringPast extends Past {

  /**
   * Timeout value which indicates that the object should never expire.
   * Note that objects with this timeout value _will_ be deleted in
   * the year 292473178. If this is a problem, applications should
   * check for this value explicitly.
   */
  public static final long INFINITY_EXPIRATION = Long.MAX_VALUE;



  /**
   * Inserts an object that must be stored until the given expiration; the application refreshes it with
   * {@link #refresh(Id[], long[])} while it is interested in it.
   *
   * @param obj the object to be inserted
   * @param expiration the time until which the object must be stored
   * @return as {@link #insert(PastContent)}
   */
  public CompletableFuture<Boolean[]> insert(PastContent obj, long expiration);

  /**
   * Inserts an object that never expires: {@code insert(obj, INFINITY_EXPIRATION)}.
   *
   * @param obj the object to be inserted
   * @return as {@link #insert(PastContent, long)}
   */
  @Override
  public CompletableFuture<Boolean[]> insert(PastContent obj);


  /**
   * Updates the objects stored under the provided keys to expire no earlier than the provided expiration times.
   *
   * @param ids The keys which to refresh
   * @param expirations The times to extend the lifetimes to, one per key
   * @return completed on the selector thread with one outcome per key, in the order of the keys: a Success
   *     (true) if the refresh went through, a Failure with an ObjectNotFoundException if no replica had the
   *     object, or with a RefreshFailedException (or the implementation's exception) if it failed otherwise
   */
  public CompletableFuture<List<Outcome<Boolean>>> refresh(Id[] ids, long[] expirations);

}
