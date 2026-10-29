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
package rice.p2p.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import rice.selector.SelectorManager;

public interface DebugCommandHandler {
  
  public String handleDebugCommand(String command);

  /**
   * Waits (blocking, with a timeout) for the text a debug command produces asynchronously.
   * <p>
   * Debug commands are run by a console thread, while their results are delivered by the selector thread. 
   * If the command is (unexpectedly) run on the selector thread and the result is not available yet, waiting
   * would deadlock, so this returns an error text instead.
   *
   * @param result completed with the command's output
   * @param selector the selector that delivers the result
   * @param timeoutMillis how long to wait, in milliseconds of wall-clock time
   * @return the command's output, or a description of why there is none
   */
  public static String await(CompletableFuture<String> result, SelectorManager selector, long timeoutMillis) {
    if (!result.isDone() && selector != null && selector.isSelectorThread()) {
      return "error: result not available, and can't wait for it on the selector thread";
    }
    try {
      return result.get(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      return "timeout: no result after "+timeoutMillis+" ms";
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return "interrupted while waiting for the result";
    } catch (ExecutionException ee) {
      return "exception("+ee.getCause()+")";
    }
  }
}
