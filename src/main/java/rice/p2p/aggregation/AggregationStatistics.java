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
package rice.p2p.aggregation;

import java.util.Arrays;

import org.slf4j.Logger;
import rice.environment.Environment;

public class AggregationStatistics {
  public final long granularity;
  private int numObjectsTotal;
  private int numObjectsAlive;
  private int numAggregatesTotal;
  private int numPointerArrays;
  private int criticalAggregates;
  private int orphanedAggregates;
  private final int[] objectLifetimeHisto;
  private final int[] aggregateLifetimeHisto;
  private long totalObjectsSize;
  private long liveObjectsSize;
  private final long time;
  private Environment environment;

  public AggregationStatistics(int histoLength, long granularityArg, Environment env) {
    this.environment = env;
    numObjectsTotal = 0;
    numObjectsAlive = 0;
    numAggregatesTotal = 0;
    numPointerArrays = 0;
    totalObjectsSize = 0;
    liveObjectsSize = 0;
    granularity = granularityArg;
    objectLifetimeHisto = new int[histoLength];
    Arrays.fill(objectLifetimeHisto, 0);
    aggregateLifetimeHisto = new int[histoLength];
    Arrays.fill(aggregateLifetimeHisto, 0);
    time = environment.getTimeSource().currentTimeMillis();
  }

  /** Counts one aggregate whose lifetime falls into the given histogram slot. */
  public void addAggregate(int numObjects, int referenceCount, int nominalReferenceCount, int lifetimeHistoIndex) {
    numAggregatesTotal ++;
    numObjectsTotal += numObjects;
    if (numObjects == 0)
      numPointerArrays ++;
    if (referenceCount < nominalReferenceCount)
      criticalAggregates ++;
    if (referenceCount < 1)
      orphanedAggregates ++;
    aggregateLifetimeHisto[lifetimeHistoIndex] ++;
  }

  /** Counts one object whose refreshed lifetime falls into the given histogram slot. */
  public void addObject(int size, boolean alive, int lifetimeHistoIndex) {
    totalObjectsSize += size;
    if (alive) {
      numObjectsAlive ++;
      liveObjectsSize += size;
    }
    objectLifetimeHisto[lifetimeHistoIndex] ++;
  }

  public void dump(Logger logger) {
    String s = "@L.AG interval="+time+"-"+environment.getTimeSource().currentTimeMillis()+" granularity="+granularity+"\n";
    s+="@L.AG   objsTotal="+numObjectsTotal+" objsAlive="+numObjectsAlive+"\n";
    s+="@L.AG   objBytesTotal="+totalObjectsSize+" objBytesAlive="+liveObjectsSize+"\n";
    s+="@L.AG   aggrTotal="+numAggregatesTotal+" ptrArrays="+numPointerArrays+" critical="+criticalAggregates+" orphaned="+orphanedAggregates;
    logger.info(s);
  }

  public int getCriticalAggregates() {
    return criticalAggregates;
  }

  public int getNumAggregatesTotal() {
    return numAggregatesTotal;
  }

  public int getNumObjectsAlive() {
    return numObjectsAlive;
  }

  public int getNumObjectsTotal() {
    return numObjectsTotal;
  }

  public int getNumPointerArrays() {
    return numPointerArrays;
  }

  public int getOrphanedAggregates() {
    return orphanedAggregates;
  }
}
