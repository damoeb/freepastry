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
 * Created on Apr 15, 2005
 */
package rice.tutorial.timer;

import java.io.IOException;

import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.tutorial.lesson3.MyMsg;

/**
 * This app shows how to trigger regularly scheduled
 * events on the FreePastry thread.
 *
 * @author Jeff Hoye
 */
public class MyApp implements Application {

  /**
   * This is the task sends the MessageToSelf every 5
   * seconds.  We keep it as a member variable to
   * cancel later.
   */
  CancellableTask messageToSelfTask;

  /**
   * The Endpoint represents the underlieing node.  By making calls on the
   * Endpoint, it assures that the message will be delivered to a MyApp on whichever
   * node the message is intended for.
   */
  private final Endpoint endpoint;

  private final Environment environment;

  /**
   * NOTE: It is unsafe to send this message to
   * anyone but the localnode.
   *
   * The reason:
   * It is a plain Message, not a RawMessage: it has
   * no serialize(OutputBuffer) and no type, so it
   * cannot go over the wire.  Endpoint.route() only
   * accepts RawMessages; scheduleMessage() delivers
   * it locally without serializing it.
   */
  static class MessageToSelf implements Message {
    @Override
    public int getPriority() {
      return MAX_PRIORITY;
    }
  }

  public MyApp(Node node) {
    this.environment = node.getEnvironment();
    // We are only going to use one instance of this application on each PastryNode
    this.endpoint = node.buildEndpoint(this, "myinstance");

    // all messages this application sends are raw; tell the endpoint how to read them back
    endpoint.setDeserializer((buf, type, priority, sender) -> {
      if (type == MyMsg.TYPE) {
        return new MyMsg(buf, endpoint);
      }
      throw new IOException("Unknown message type: " + type);
    });

    endpoint.register();

    // Send MessageToSelf every 5 seconds, starting in 3 seconds
    messageToSelfTask = endpoint.scheduleMessage(new MessageToSelf(), 3000, 5000);
  }

  /**
   * Call this to cancel the task.
   */
  public void cancelTask() {
    messageToSelfTask.cancel();
  }



  /**
   * Called to route a message to the id
   */
  public void routeMyMsg(Id id) {
    System.out.println(this+" sending to "+id);
    MyMsg msg = new MyMsg(endpoint.getId(), id);
    endpoint.route(id, msg, null);
  }

  /**
   * Called to directly send a message to the node handle
   */
  public void routeMyMsgDirect(NodeHandle nodeHandle) {
    System.out.println(this+" sending direct to "+nodeHandle);
    MyMsg msg = new MyMsg(endpoint.getId(), nodeHandle.getId());
    endpoint.route(null, msg, nodeHandle);
  }

  /**
   * Called when we receive a message.
   */
  @Override
  public void deliver(Id id, Message message) {
    System.out.println(this+" received "+message);
    if (message instanceof MessageToSelf) {
      // This will get called every 5 seconds, on Pastry's thread.
      // Thus now we can assume we are on Pastry's thread.
      // This is where your application does its periodic work, e.g. send messages or print its status.
      System.out.println("I got the MessageToSelf at time:"+environment.getTimeSource().currentTimeMillis());
    }
  }

  /**
   * Called when you hear about a new neighbor.
   * Don't worry about this method for now.
   */
  @Override
  public void update(NodeHandle handle, boolean joined) {
  }

  /**
   * Called a message travels along your path.
   * Don't worry about this method for now.
   */
  @Override
  public boolean forward(RouteMessage message) {
    return true;
  }

  @Override
  public String toString() {
    return "MyApp "+endpoint.getId();
  }

}
