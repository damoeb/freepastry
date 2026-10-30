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

package rice.persistence;

import java.util.function.Supplier;

import org.slf4j.Logger;
import rice.environment.time.TimeSource;

/**
 * Counts the requests of a {@link PersistentStorage} and logs the counts once per interval.
 */
final class StorageStatistics {

  /**
   * Fields for logging based on the requests we are writing.
   */
  private final Object statLock = new Object();
  private long statsLastWritten;
  private final long statsWriteInterval = 60 * 1000;
  private long numWrites = 0;
  private long numReads = 0;
  private long numRenames = 0;
  private long numDeletes = 0;
  private long numMetadataWrites = 0;

  private final String name;
  private final TimeSource timeSource;
  private final Logger logger;

  StorageStatistics(String name, TimeSource timeSource, Logger logger) {
    this.name = name;
    this.timeSource = timeSource;
    this.logger = logger;
    this.statsLastWritten = timeSource.currentTimeMillis();
  }

  /**
   * Logs the counts if a new interval has started since they were last logged.
   *
   * @param objsTotal the number of stored objects, or "?" if unknown
   * @param objsBytesTotal the number of bytes stored
   */
  void printStats(Supplier<String> objsTotal, Supplier<Long> objsBytesTotal) {
    synchronized(statLock) {
      long now = timeSource.currentTimeMillis();
      if ((statsLastWritten/statsWriteInterval) != (now/statsWriteInterval)) {
        logger.info("@L.PE name={} interval={}-{}", name, statsLastWritten, now);
        statsLastWritten = now;

        logger.atInfo().log(() -> "@L.PE   objsTotal=" + objsTotal.get() + " objsBytesTotal=" + objsBytesTotal.get());
        logger.info("@L.PE   numWrites={} numReads={} numDeletes={}", numWrites, numReads, numDeletes);
        logger.info("@L.PE   numMetadataWrites={} numRenames={}", numMetadataWrites, numRenames);
      }
    }
  }

  void countWrite() {
    synchronized(statLock) { numWrites++; }
  }

  void countRead() {
    synchronized(statLock) { numReads++; }
  }

  void countRename() {
    synchronized(statLock) { numRenames++; }
  }

  void countDelete() {
    synchronized(statLock) { numDeletes++; }
  }

  void countMetadataWrite() {
    synchronized(statLock) { numMetadataWrites++; }
  }
}
