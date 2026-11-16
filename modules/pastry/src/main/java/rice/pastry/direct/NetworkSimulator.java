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
package rice.pastry.direct;

import java.util.concurrent.CopyOnWriteArrayList;
import org.mpisws.p2p.transport.direct.DirectTransportLayer;
import org.mpisws.p2p.transport.direct.GenericNetworkSimulator;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;

/**
 * Interface to an object which is simulating the network.
 *
 * @version $Id: NetworkSimulator.java 4221 2008-05-19 16:41:19Z jeffh $
 *
 * @author Andrew Ladd
 */
public class NetworkSimulator<Identifier, MessageType> {
  private final BasicNetworkSimulator<Identifier, MessageType> simulator;
  private final RandomSource random;
  private final ProximityGenerator generator;
  private final LivenessProvider<Identifier> livenessProvider;

  // TODO: add a listener to the top-level transport layer to notify simulator listeners
  public NetworkSimulator(Environment env, ProximityGenerator generator) {
    NetworkSimulatorConfig config = NetworkSimulatorConfig.fromParameters(env.getParameters());
    if (config.useOwnRandom()) {
      if (config.randomSeed().isPresent()) {
        this.random = new SimpleRandomSource(config.randomSeed().getAsLong(), env.getLogManager(),
            "direct");
      } else {
        this.random = new SimpleRandomSource(env.getLogManager(), "direct");
      }
    } else {
      this.random = env.getRandomSource();
    }
    generator.setRandom(random);
    this.generator = generator;
    simulator = new BasicNetworkSimulator<Identifier, MessageType>(env, random, this, config);
    livenessProvider = simulator;
  }

  /**
   * unlimited maxSpeed
   *
   */
  public void setFullSpeed() {
    simulator.setFullSpeed();
  }

  /**
   * The max rate of the simulator compared to realtime.
   *
   * The rule is that the simulated clock will not be set to a value greater
   * than the factor from system-time that the call was made.  Thus
   *
   * if 1 hour ago, you said the simulator should run at 10x realtime the simulated
   * clock will only have advanced 10 hours.
   *
   * Note that if the simulator cannot keep up with the system clock in the early
   * part, it may move faster than the value you set to "catch up"
   *
   * To prevent this speed-up from becoming unbounded, you may wish to call
   * setMaxSpeed() periodically or immediately after periods of expensive calculations.
   *
   * Setting the simulation speed to zero will not pause the simulation, you must
   * call stop() to do that.
   *
   * @param rate the multiple on realtime that the simulator is allowed to run at,
   * zero or less will cause no bound on the simulation speed
   *
   */
  public void setMaxSpeed(float rate) {
    simulator.setMaxSpeed(rate);
  }

  public void start() {
    simulator.start();
  }

  public void stop() {
    simulator.stop();
  }

  // ************************* What is this? **********************
  private TestRecord testRecord;
  /**
   * get TestRecord
   *
   * @return the returned TestRecord
   */
  public TestRecord getTestRecord() {
    return testRecord;
  }

  /**
   * set TestRecord
   *
   * @param tr input TestRecord
   */
  public void setTestRecord(TestRecord tr) {
    testRecord = tr;
  }


  /************** SimulatorListeners handling *******************/
  final CopyOnWriteArrayList<GenericSimulatorListener<Identifier, MessageType>> listeners = new CopyOnWriteArrayList<GenericSimulatorListener<Identifier, MessageType>>();
  /**
   *
   * @return true if added, false if already a listener
   */
  public boolean addSimulatorListener(GenericSimulatorListener<Identifier, MessageType> sl) {
    return listeners.addIfAbsent(sl);
  }

  /**
   *
   * @return true if removed, false if not already a listener
   */
  public boolean removeSimulatorListener(GenericSimulatorListener<Identifier, MessageType> sl) {
    return listeners.remove(sl);
  }

  /**
   * Call this when a message is sent.
   * @param m the message
   * @param from the source
   * @param to the destination
   * @param delay the network proximity (when the message will be received)
   */
  public void notifySimulatorListenersSent(MessageType m, Identifier from, Identifier to, int delay) {
    for (GenericSimulatorListener<Identifier, MessageType> listener : listeners) {
      listener.messageSent(m, from, to, delay);
    }
  }

  /**
   * Call this when a message is received.
   * @param m the message
   * @param from the source
   * @param to the destination
   */
  public void notifySimulatorListenersReceived(MessageType m, Identifier from, Identifier to) {
    for (GenericSimulatorListener<Identifier, MessageType> listener : listeners) {
      listener.messageReceived(m, from, to);
    }
  }

  /**
   * Generates a random node record
   *
   */
  public NodeRecord generateNodeRecord() {
    return generator.generateNodeRecord();
  }

  /**
   * Returns the closest Node in proximity.
   *
   * @param nh the node to find the closest node to
   * @return the closest live, ready node
   */
  public DirectNodeHandle getClosest(DirectNodeHandle nh) {
    return null;
  }

  /**
   * Checks to see if a node id is alive.
   *
   * @param nh a node.
   *
   * @return true if alive, false otherwise.
   */
  public boolean isAlive(Identifier nh) {
    return simulator.isAlive(nh);
  }

  /**
   * Determines delivery time from a to b.
   *
   * @param a a node id.
   * @param b another node id.
   *
   * @return proximity of b to a.
   */
  public float networkDelay(Identifier a, Identifier b) {
    return simulator.networkDelay(a, b);
  }

  /**
   * Determines rtt between two nodes.
   *
   * @param a a node id.
   * @param b another node id.
   *
   * @return proximity of b to a.
   */
  public float proximity(Identifier a, Identifier b) {
    return simulator.proximity(a, b);
  }

  public NodeRecord getNodeRecord(DirectNodeHandle handle) {
    return simulator.getNodeRecord(handle);
  }

  public LivenessProvider<Identifier> getLivenessProvider() {
    return livenessProvider;
  }

  public GenericNetworkSimulator<Identifier, MessageType> getGenericSimulator() {
    return simulator;
  }

  public void registerNode(Identifier i, DirectTransportLayer<Identifier, MessageType> dtl, NodeRecord nr) {
    simulator.registerIdentifier(i, dtl, nr);
  }
}
