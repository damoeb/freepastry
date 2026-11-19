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
/*
 * Created on Dec 11, 2006
 */
package rice.pastry.leafset.testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.proximity.ProximityListener;
import rice.environment.Environment;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeSetEventSource;
import rice.pastry.NodeSetListener;
import rice.pastry.PastryNode;
import rice.pastry.ScheduledMessage;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.client.PastryAppl;
import rice.pastry.leafset.LeafSet;
import rice.pastry.messaging.Message;
import rice.pastry.messaging.PastryRawMessage;
import rice.pastry.routing.RoutingTable;
import rice.pastry.transport.PastryMessageReceipt;
import rice.pastry.transport.PastryMessageSendListener;

public class MergeTest {


  /**
   * Input of the form:
   * <0xD74D4F..><0xD7B075..><0xD98A9D..><0xDAC7F0..><0xDB39A6..><0xDD5A73..><0xE050B3..><0xE0B735..><0xE33A04..><0xE48D40..><0xE678CB..><0xE73F09..> [ <0xEA5EAF..> ] <0xEBC2BB..><0xEBD2CB..><0xEF7F43..><0xF09044..><0xF10B96..><0xF33C36..><0xF64DA9..><0xF66CD9..><0xF9E251..><0xFB7F46..><0xFC1B02..><0xFC4718..>
   */
  @SuppressWarnings("StringSplitter") // dropping trailing empty entries is intended
  public static LeafSet getLeafSet(String str) {
    String a[] = str.split("\\[");
    assert(a.length == 2);
    String b[] = a[1].split("]");
    assert(b.length == 2);

    String s_ccw = a[0]; // <0xD74D4F..><0xD7B075..><0xD98A9D..><0xDAC7F0..><0xDB39A6..><0xDD5A73..><0xE050B3..><0xE0B735..><0xE33A04..><0xE48D40..><0xE678CB..><0xE73F09..>
    String s_cw = b[1];
    String s_base = b[0]; //<0xEA5EAF..>

    NodeHandle[] counterClockwiseHandles = getHandles(s_ccw);
    flip(counterClockwiseHandles);
    NodeHandle base = getHandles(s_base)[0];
    NodeHandle[] clockwiseHandles = getHandles(s_cw);

    LeafSet leafSet = new LeafSet(base,24,true,clockwiseHandles,counterClockwiseHandles);

    return leafSet;
  }

  public static void flip(NodeHandle[] nds) {
    for (int a = 0; a < nds.length/2; a++) {
      int b = nds.length-a-1;
      NodeHandle temp = nds[a];
      nds[a] = nds[b];
      nds[b] = temp;
    }
  }

  /**
   * Input of the form:
   * <0xD74D4F..><0xD7B075..><0xD98A9D..><0xDAC7F0..><0xDB39A6..>
   */
  @SuppressWarnings("StringSplitter") // dropping trailing empty entries is intended
  public static NodeHandle[] getHandles(String str) {
    ArrayList<NodeHandle> list = new ArrayList<NodeHandle>();
    String a[] = str.split("[< ]");
    for (int counter = 0; counter < a.length; counter++) {
      if (a[counter].length() > 3) {
        assert a[counter].substring(0,2).equals("0x");
        assert a[counter].substring(a[counter].length()-3,a[counter].length()).equals("..>");
        a[counter] = a[counter].substring(2,a[counter].length()-3);

        list.add(new TestNodeHandle(Id.build(a[counter])));

      }
    }
    return list.toArray(new NodeHandle[0]);
  }

  public static void main(String[] args) {
    Environment env = new Environment();

//  leafset:  [ <0xEA1020..> ]  complete:false size:0 s2:false.merge(leafset: <0xD74D4F..><0xD7B075..><0xD98A9D..><0xDAC7F0..><0xDB39A6..><0xDD5A73..><0xE050B3..><0xE0B735..><0xE33A04..><0xE48D40..><0xE678CB..><0xE73F09..> [ <0xEA5EAF..> ] <0xEBC2BB..><0xEBD2CB..><0xEF7F43..><0xF09044..><0xF10B96..><0xF33C36..><0xF64DA9..><0xF66CD9..><0xF9E251..><0xFB7F46..><0xFC1B02..><0xFC4718..> complete:true size:24 s1:false s2:false,[SNH: <0xEA5EAF..>//128.59.20.228:21854 [-7262332366877176307]],...,false,null)
    String s_ls1 = "<0xD74D4F..><0xD7B075..><0xD98A9D..><0xDAC7F0..><0xDB39A6..><0xDD5A73..><0xE050B3..><0xE0B735..><0xE33A04..><0xE48D40..><0xE678CB..><0xE73F09..> [ <0xEA5EAF..> ] <0xEBC2BB..><0xEBD2CB..><0xEF7F43..><0xF09044..><0xF10B96..><0xF33C36..><0xF64DA9..><0xF66CD9..><0xF9E251..><0xFB7F46..><0xFC1B02..><0xFC4718..>";

    LeafSet firstLeafSet = getLeafSet(s_ls1);

    String s_ls2 = " [ <0xEA1020..> ] ";

    LeafSet secondLeafSet = getLeafSet(s_ls2);

    PastryNode pn = new PastryNode((rice.pastry.Id)secondLeafSet.get(0).getId(),env){

      @Override
      public NodeHandle readNodeHandle(InputBuffer buf) throws IOException {
        return null;
      }

      @Override
      public PastryMessageReceipt send(NodeHandle handle, PastryRawMessage message, PastryMessageSendListener deliverAckToMe, Map<String, Object> options) {
        return null;
      }

      @Override
      public ScheduledMessage scheduleMessageAtFixedRate(Message msg, long delay, long period) {
        return null;
      }

      @Override
      public ScheduledMessage scheduleMessage(Message msg, long delay) {
        return null;
      }

      @Override
      public ScheduledMessage scheduleMessage(Message msg, long delay, long period) {
        return null;
      }

      @Override
      public int proximity(NodeHandle nodeHandle) {
        return 0;
      }

      @Override
      public int proximity(NodeHandle nodeHandle, Map<String, Object> options) {
        return 0;
      }

      @Override
      public SocketRequestHandle<NodeHandle> connect(NodeHandle handle, AppSocketReceiver receiver, PastryAppl appl, int timeout) {
        return null;
      }

      @Override
      public NodeHandle coalesce(NodeHandle newHandle) {
        return null;
      }

      @Override
      public <Identifier> Bootstrapper<Identifier> getBootstrapper() {

        return null;
      }

      @Override
      public void addLivenessListener(LivenessListener<NodeHandle> name) {

      }

      @Override
      public boolean checkLiveness(NodeHandle i, Map<String, Object> options) {
        return false;
      }

      @Override
      public int getLiveness(NodeHandle i, Map<String, Object> options) {
        return 0;
      }

      @Override
      public boolean removeLivenessListener(LivenessListener<NodeHandle> name) {
        return false;
      }

      @Override
      public void clearState(NodeHandle i) {

      }

      @Override
      public void addProximityListener(ProximityListener<NodeHandle> listener) {

      }

      @Override
      public boolean removeProximityListener(ProximityListener<NodeHandle> listener) {
        return false;
      }

      @Override
      public String printRouteState() {
        return null;
      }
    };

    RoutingTable routingTable = new RoutingTable(secondLeafSet.get(0),1,(byte)4,pn);

    secondLeafSet.addNodeSetListener(new NodeSetListener() {

      @Override
      public void nodeSetUpdate(NodeSetEventSource nodeSetEventSource,
          NodeHandle handle, boolean added) {
        System.out.println("nodeSetUpdate("+handle+","+added+")");

      }
    });

    secondLeafSet.merge(firstLeafSet,firstLeafSet.get(0),routingTable,false, null);


    env.destroy();

  }

  public static class TestNodeHandle extends NodeHandle {
    private Id id;

    public TestNodeHandle(Id id) {
      this.id = id;
    }

    @Override
    public Id getNodeId() {
      return id;
    }

    @Override
    public int getLiveness() {
      return NodeHandle.LIVENESS_ALIVE;
    }

    @Override
    public boolean ping() {
      return true;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TestNodeHandle testNodeHandle) {
        return testNodeHandle.id.equals(id);
      }

      return false;
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    @Override
    public String toString() {
      return id.toString();
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      throw new RuntimeException("not implemented.");
    }
  }


}
