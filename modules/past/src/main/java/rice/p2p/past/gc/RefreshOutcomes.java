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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import rice.Outcome;

/**
 * Converts between the per-id result of {@link ExpiringPast#refresh(rice.p2p.commonapi.Id[], long[])}, one
 * {@link Outcome} per id, and the {@code Object[]} of the {@code Continuation} based refresh that glacier and
 * aggregation still use internally ({@code Boolean} or an exception per id). Only for that boundary: it goes once
 * that code takes the outcomes.
 */
public final class RefreshOutcomes {

  private RefreshOutcomes() {
  }

  /**
   * @param results per id a Boolean (or null) for a refresh that went through, or the exception it failed with
   * @return per id a Success with that Boolean, or a Failure with that exception
   */
  public static List<Outcome<Boolean>> fromArray(Object[] results) {
    List<Outcome<Boolean>> outcomes = new ArrayList<Outcome<Boolean>>(results.length);
    for (Object result : results) {
      outcomes.add(switch (result) {
        case null -> new Outcome.Success<Boolean>(null);
        case Boolean isRefreshed -> new Outcome.Success<Boolean>(isRefreshed);
        case Throwable failure -> new Outcome.Failure<Boolean>(failure);
        default -> new Outcome.Failure<Boolean>(new IllegalStateException("not a refresh result: " + result));
      });
    }
    return Collections.unmodifiableList(outcomes);
  }

  /**
   * @param outcomes per id the outcome of its refresh
   * @return per id the Boolean of a Success or the exception of a Failure
   */
  public static Object[] toArray(List<Outcome<Boolean>> outcomes) {
    Object[] results = new Object[outcomes.size()];
    for (int i = 0; i < results.length; i++) {
      results[i] = switch (outcomes.get(i)) {
        case Outcome.Success<Boolean> success -> success.value();
        case Outcome.Failure<Boolean> failure -> failure.cause();
      };
    }
    return results;
  }
}
