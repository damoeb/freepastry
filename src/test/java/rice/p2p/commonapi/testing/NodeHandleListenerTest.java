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
package rice.p2p.commonapi.testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleListener;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the NodeHandleListener mechanism of NodeHandle.
 */
public class NodeHandleListenerTest {

  static class TestHandle extends NodeHandle {
    private static final long serialVersionUID = 1L;

    public Id getId() { return null; }
    @Deprecated
    public boolean isAlive() { return true; }
    @Deprecated
    public int proximity() { return 0; }
    public boolean checkLiveness() { return true; }
    public void serialize(OutputBuffer buf) throws IOException { }

    void fire(Object event) {
      notifyNodeHandleListeners(event);
    }
  }

  static class Recorder implements NodeHandleListener {
    final String name;
    final List<String> log;

    Recorder(String name, List<String> log) {
      this.name = name;
      this.log = log;
    }

    public void nodeHandleUpdate(NodeHandle handle, Object event) {
      log.add(name + ":" + event);
    }
  }

  @Test
  void notifiesByPriorityThenRegistrationOrder() {
    TestHandle handle = new TestHandle();
    List<String> log = new ArrayList<String>();
    handle.addNodeHandleListener(new Recorder("a", log));
    handle.addNodeHandleListener(new Recorder("high", log), 50);
    handle.addNodeHandleListener(new Recorder("b", log));
    handle.addNodeHandleListener(new Recorder("mid", log), 40);

    handle.fire(NodeHandle.DECLARED_DEAD);

    assertEquals(List.of("high:2", "mid:2", "a:2", "b:2"), log);
  }

  @Test
  void readdingWithOtherPriorityOnlyChangesPriority() {
    TestHandle handle = new TestHandle();
    List<String> log = new ArrayList<String>();
    Recorder a = new Recorder("a", log);
    handle.addNodeHandleListener(a);
    handle.addNodeHandleListener(new Recorder("b", log));
    handle.addNodeHandleListener(a, 10);

    handle.fire(NodeHandle.DECLARED_LIVE);

    assertEquals(List.of("a:3", "b:3"), log);
    assertEquals(2, handle.countNodeHandleListeners());
  }

  @Test
  void removeDuringNotificationIsSafe() {
    TestHandle handle = new TestHandle();
    List<String> log = new ArrayList<String>();
    handle.addNodeHandleListener(new NodeHandleListener() {
      public void nodeHandleUpdate(NodeHandle h, Object event) {
        log.add("once:" + event);
        assertTrue(h.removeNodeHandleListener(this));
      }
    });
    handle.addNodeHandleListener(new Recorder("always", log));

    handle.fire(NodeHandle.DECLARED_DEAD);
    handle.fire(NodeHandle.DECLARED_LIVE);

    assertEquals(List.of("once:2", "always:2", "always:3"), log);
    assertFalse(handle.removeNodeHandleListener(new Recorder("unknown", log)));
  }
}
