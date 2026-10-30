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
package rice.selector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimerTaskTest {

  static class Task extends TimerTask {
    Task(long nextExecutionTime) {
      setNextExecutionTime(nextExecutionTime);
    }

    public void run() {
    }
  }

  @Test
  public void ordersTasksFarApartByTime() {
    // 2^32 ms apart: the low 32 bits of the difference are 0
    Task early = new Task(0);
    Task late = new Task(1L << 32);
    assertTrue(early.compareTo(late) < 0);
    assertTrue(late.compareTo(early) > 0);

    // 2^31 ms (24.8 days) apart: the difference truncated to an int is negative
    Task later = new Task(1L << 31);
    assertTrue(early.compareTo(later) < 0);
    assertTrue(later.compareTo(early) > 0);
  }
}
