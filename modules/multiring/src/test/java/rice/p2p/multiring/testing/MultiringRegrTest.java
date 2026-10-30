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

package rice.p2p.multiring.testing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.multiring.MultiringNode;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.direct.DirectNodeHandle;
import rice.pastry.direct.DirectPastryNodeFactory;
import rice.pastry.direct.EuclideanNetwork;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.direct.SphereNetwork;
import rice.pastry.dist.DistPastryNodeFactory;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.pastry.transport.BogusNodeHandle;

/**
 * Provides regression testing setup for applications written on top of the
 * commonapi.  Currently is written to use Pastry nodes, but this will be abstracted
 * away.
 *
 * @version $Id: MultiringRegrTest.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public class MultiringRegrTest {

  // ----- VARAIBLES -----

  // the collection of nodes which have been created
  protected MultiringNode[] globalNodes;

  // the collection of nodes which have been created
  protected MultiringNode[][] organizationalNodes;

  // the test applications on the global nodes
  protected MultiringTestApp[] globalApps;

  // the collection of apps on org nodes
  protected MultiringTestApp[][] organizationalApps;

  // the global ring id
  protected Id globalRingId;

  // the list of ringIds for organizations
  protected Id[] ringIds;

  // ----- PASTRY SPECIFIC VARIABLES -----

  // the factory for creating pastry nodes
  protected PastryNodeFactory factory;

  // the factory for creating random node ids
  protected IdFactory idFactory;

  // the simulator, in case of direct
  protected NetworkSimulator<DirectNodeHandle, RawMessage> simulator;


  // ----- STATIC FIELDS -----

  // the number of nodes to create
  public static int NUM_GLOBAL_NODES = 20;

  // the number of organizations to create
  public static int NUM_ORGANIZATIONS = 5;

  // the number of internal nodes in each organization
  public static int NUM_INTERNAL_NODES = 3;

  // the number gateway nodes in each org
  public static int NUM_GATEWAY_NODES = NUM_GLOBAL_NODES / NUM_ORGANIZATIONS;

  // the number of nodes in each organization
  public static int NUM_ORGANIZATIONAL_NODES = NUM_GATEWAY_NODES + NUM_INTERNAL_NODES;

  // ----- TESTING SPECIFIC FIELDS -----

  // the text to print to the screen
  public static final String SUCCESS = "SUCCESS";
  public static final String FAILURE = "FAILURE";

  // the width to pad the output
  protected static final int PAD_SIZE = 60;

  // the direct protocol
  public static final int PROTOCOL_DIRECT = -138;

  // the possible network simulation models
  public static final int SIMULATOR_SPHERE = -1;
  public static final int SIMULATOR_EUCLIDEAN = -2;


  // ----- PASTRY SPECIFIC FIELDS -----

  // the port to begin creating nodes on
  public static int PORT = 5009;

  // the host to boot the first node off of
  public static String BOOTSTRAP_HOST = "localhost";

  // the port on the bootstrap to contact
  public static int BOOTSTRAP_PORT = 5009;

  // the procotol to use when creating nodes
  public static int PROTOCOL = DistPastryNodeFactory.PROTOCOL_DEFAULT;

  // the simulator to use in the case of direct
  public static int SIMULATOR = SIMULATOR_SPHERE;

  // the instance name to use
  public static String INSTANCE_NAME = "MultiringRegrTest";


  // ----- ATTEMPT TO LOAD LOCAL HOSTNAME -----

  static {
    try {
      BOOTSTRAP_HOST = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      System.out.println("Error determining local host: " + e);
    }
  }

  public Environment environment;

  // ----- EXTERNALLY AVAILABLE METHODS -----

  /**
   * Constructor, which takes no arguments and sets up the
   * factories in preparation for node creation.
   */
  private final List<AssertionError> failures = Collections.synchronizedList(new ArrayList<AssertionError>());

  public MultiringRegrTest(Environment env) throws IOException {
    this.environment = env;

    if (PROTOCOL == PROTOCOL_DIRECT) {
      if (SIMULATOR == SIMULATOR_SPHERE) {
        simulator = new SphereNetwork<DirectNodeHandle, RawMessage>(env);
      } else {
        simulator = new EuclideanNetwork<DirectNodeHandle, RawMessage>(env);
      }
      factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(environment), simulator, env);
    } else {
      factory = DistPastryNodeFactory.getFactory(new RandomNodeIdFactory(environment),
                                                 PROTOCOL,
                                                 PORT,
                                                 env);
    }

    NUM_GATEWAY_NODES = NUM_GLOBAL_NODES / NUM_ORGANIZATIONS;
    NUM_ORGANIZATIONAL_NODES = NUM_GATEWAY_NODES + NUM_INTERNAL_NODES;

    idFactory = new PastryIdFactory(env);
    globalRingId = idFactory.buildId(new byte[20]);
    ringIds = new Id[NUM_ORGANIZATIONS];
    globalNodes = new MultiringNode[NUM_GLOBAL_NODES];
    organizationalNodes = new MultiringNode[NUM_ORGANIZATIONS][NUM_ORGANIZATIONAL_NODES];
    globalApps = new MultiringTestApp[NUM_GLOBAL_NODES];
    organizationalApps = new MultiringTestApp[NUM_ORGANIZATIONS][NUM_ORGANIZATIONAL_NODES];
  }

  /**
   * Method which creates the nodes
   */
  public void createNodes() {
    for (int i=0; i<NUM_GLOBAL_NODES; i++) {
      globalNodes[i] = createNode(globalRingId, globalNodes[0]);

      simulate();

      globalApps[i] = new MultiringTestApp(globalNodes[i]);
      simulate();

      System.out.println("Created node " + i + " in the global ring with id " + globalNodes[i].getId());
    }

    for (int i=0; i<NUM_ORGANIZATIONS; i++) {
      ringIds[i] = generateId(16 * (i+1));

      for (int j=0; j<NUM_GATEWAY_NODES; j++) {
        organizationalNodes[i][j] = createNode(globalNodes[i*NUM_GATEWAY_NODES + j], ringIds[i], organizationalNodes[i][0]);
        simulate();

        organizationalApps[i][j] = new MultiringTestApp(organizationalNodes[i][j]);
        simulate();

        System.out.println("Created gateway node " + j + " in ring " + ringIds[i] + " with id " + organizationalNodes[i][j].getId());
      }

      for (int j=NUM_GATEWAY_NODES; j< NUM_ORGANIZATIONAL_NODES; j++) {
        organizationalNodes[i][j] = createNode(ringIds[i], organizationalNodes[i][0]);
        simulate();

        organizationalApps[i][j] = new MultiringTestApp(organizationalNodes[i][j]);
        simulate();

        System.out.println("Created internal node " + (j-NUM_GATEWAY_NODES) + " in ring " + ringIds[i] + " with id " + organizationalNodes[i][j].getId());
      }
    }
  }

  /**
   * Method which starts the creation of nodes
   */
  public void start() {
    createNodes();

    System.out.println("\nTest Beginning\n");

    runTest();
  }


  // ----- INTERNAL METHODS -----

  /**
   * Gives the nodes 300 ms to pass messages.
   */
  protected void simulate() {
    try { Thread.sleep(300); } catch (InterruptedException ie) {}
  }

  /**
   * Method which creates a non-gateway node
   *
   * @param ringId The ring to create the node in
   * @param bootstrap The node to boot off of, or null to start a new ring
   * @return The created node
   */
  protected MultiringNode createNode(Id ringId, MultiringNode bootstrap) {
    MultiringNode mn;

    if (bootstrap == null) {
      mn = new MultiringNode(ringId, newBootedNode(null, null));
    } else {
      mn = new MultiringNode(ringId, newBootedNode(getBootstrap(bootstrap.getNode()), null));
    }

    PastryNode pn = (PastryNode)mn.getNode();
    synchronized(pn) {
      while(!pn.isReady()) {
        try {
          pn.wait(500);
        } catch (InterruptedException ie) {
          return null;
        }
      }
      if (!pn.isReady()) {
        System.out.println("Still waiting for node "+pn+" in ring "+ringId+" to be ready.");
      }
    }
    return mn;
  }

  /**
   * Method which creates a gateway node
   *
   * @param existing The existing node in another ring; the new node shares its node id and collection
   * @param ringId The ring to create the node in
   * @param bootstrap The node to boot off of, or null to start a new ring
   * @return The created node
   */
  protected MultiringNode createNode(MultiringNode existing, Id ringId, MultiringNode bootstrap) {
    if (existing == null)
      throw new IllegalArgumentException("EXISTING WAS NULL! " + ringId + " " + bootstrap);

    if (bootstrap == null) {
      return new MultiringNode(ringId, newBootedNode(null, (rice.pastry.Id)existing.getNodeId()), existing);
    } else {
      return new MultiringNode(ringId, newBootedNode(getBootstrap(bootstrap.getNode()), (rice.pastry.Id)existing.getNodeId()), existing);
    }
  }

  /**
   * Creates a new node (with the given nodeId, or a random one if null) and boots it
   * off of the given bootstrap handle (or starts a new ring if null).
   */
  protected PastryNode newBootedNode(rice.pastry.NodeHandle bootstrap, rice.pastry.Id nodeId) {
    PastryNode pn;
    try {
      pn = (nodeId == null) ? factory.newNode() : factory.newNode(nodeId);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    if (bootstrap instanceof BogusNodeHandle bogusNodeHandle) {
      // socket: the bootstrapper expects the addresses
      pn.boot(bogusNodeHandle.getAddress());
    } else {
      pn.boot(bootstrap);
    }
    return pn;
  }

  /**
   * Gets a handle to a bootstrap node.
   *
   * @return handle to bootstrap node, or null.
   */
  protected rice.pastry.NodeHandle getBootstrap(Node bootstrap) {
    if (PROTOCOL == PROTOCOL_DIRECT) {
      return ((PastryNode) bootstrap).getLocalHandle();
    } else {
      return ((SocketPastryNodeFactory) factory).getNodeHandle(((SocketNodeHandle) ((PastryNode) bootstrap).getLocalHandle()).getAddress().getInnermostAddress());
    }
  }

  /**
   * Method which pauses for the provided number of milliseconds
   *
   * @param ms The number of milliseconds to pause
   */
  protected synchronized void pause(int ms) {
    if (PROTOCOL != PROTOCOL_DIRECT)
      try { wait(ms); } catch (InterruptedException e) {}
  }

  /**
   * Private method which generates a Id with a prefix
   *
   * @return A new random Id
   */
  private Id generateId(int i) {
    byte[] data = new byte[20];
    data[data.length - 1] = (byte) 2;
    data[data.length - 2] = (byte) i;
    return idFactory.buildId(data);
  }


  // ----- METHODS TO BE PROVIDED BY IMPLEMENTATIONS -----

  /**
   * Method which should run the test - this is called once all of the
   * nodes have been created and are ready.
   */
  protected void runTest() {
    RandomSource rng = environment.getRandomSource();
    for (int i=0; i<20; i++) {
      int si = rng.nextInt(NUM_ORGANIZATIONS);
      int sj = rng.nextInt(NUM_ORGANIZATIONAL_NODES);
      int di = rng.nextInt(NUM_ORGANIZATIONS);
      int dj = rng.nextInt(NUM_ORGANIZATIONAL_NODES);
      MultiringTestApp sourceApp = organizationalApps[si][sj];
      Id source = organizationalNodes[si][sj].getId();
      Id dest = organizationalNodes[di][dj].getId();

      System.out.println(i+" SENDING FROM " + source + " TO " + dest);
      sourceApp.send(dest);

      try {
        Thread.sleep(500);
      } catch (Exception e) {}
    }
  }

  public static class MultiringTestApp implements Application {

    protected Endpoint endpoint;

    public MultiringTestApp(Node node) {
      this.endpoint = node.buildEndpoint(this, "BLAH");
      this.endpoint.setDeserializer((buf, type, priority, sender) -> {
        if (type == MultiringTestMessage.TYPE) {
          return new MultiringTestMessage(buf, endpoint);
        }
        throw new IOException("Unknown message type: " + type);
      });
      this.endpoint.register();
    }

    public void send(Id target) {
      endpoint.route(target, new MultiringTestMessage(endpoint.getId()), null);
    }

    @Override
    public boolean forward(RouteMessage message) {
      return true;
    }

    @Override
    public void deliver(Id id, Message message) {
      System.out.println("RECEIVED MESSSAGE FROM " + ((MultiringTestMessage) message).source + " FOR TARGET " + id + " AT NODE " + endpoint.getId());
    }

    @Override
    public void update(NodeHandle handle, boolean joined) {
    }
  }

  public static class MultiringTestMessage implements RawMessage {
    public static final short TYPE = 1;

    public Id source;

    public MultiringTestMessage(Id source) {
      this.source = source;
    }

    MultiringTestMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
      source = endpoint.readId(buf, buf.readShort());
    }

    @Override
    public int getPriority() {
      return MEDIUM_PRIORITY;
    }

    @Override
    public short getType() {
      return TYPE;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeShort(source.getType());
      source.serialize(buf);
    }
  }


  // ----- TESTING UTILITY METHODS -----

  /**
   * Method which prints the beginning of a test section.
   *
   * @param name The name of section
   */
  protected final void sectionStart(String name) {
    System.out.println(name);
  }

  /**
   * Method which prints the end of a test section.
   */
  protected final void sectionDone() {
    System.out.println();
  }

  /**
   * Method which prints the beginning of a test section step.
   *
   * @param name The name of step
   */
  protected final void stepStart(String name) {
    System.out.print(pad("  " + name));
  }

  /**
   * Method which prints the end of a test section step, with an
   * assumed success.
   */
  protected final void stepDone() {
    stepDone(SUCCESS);
  }

  /**
   * Method which prints the end of a test section step.
   *
   * @param status The status of step
   */
  protected final void stepDone(String status) {
    stepDone(status, "");
  }

  /**
   * Method which prints the end of a test section step, as
   * well as a message.
   *
   * @param status The status of section
   * @param message The message
   */
  protected final void stepDone(String status, String message) {
    System.out.println("[" + status + "]");

    if ((message != null) && ! message.equals("")) {
      System.out.println("     " + message);
    }

    if(status.equals(FAILURE))
      fail(new AssertionError("Step failed: " + message));
  }

  /**
   * Records the failure, see {@link #getFailures()}, and throws it to stop the test (it used to call
   * System.exit(0), even on failure). Steps can fail on another thread, hence the record.
   */
  private void fail(AssertionError error) {
    failures.add(error);
    throw error;
  }

  /**
   * The failed steps.
   */
  public List<AssertionError> getFailures() {
    synchronized (failures) {
      return new ArrayList<AssertionError>(failures);
    }
  }

  /**
   * Method which prints an exception which occured during testing.
   *
   * @param e The exception which was thrown
   */
  protected final void stepException(Exception e) {
    System.out.println("\nException " + e + " occurred during testing.");

    e.printStackTrace();
    fail(new AssertionError("Exception occurred during testing", e));
  }

  /**
   * Method which pads a given string with "." characters.
   *
   * @param start The string
   * @return The result.
   */
  private final String pad(String start) {
    if (start.length() >= PAD_SIZE) {
      return start.substring(0, PAD_SIZE);
    } else {
      int spaceLength = PAD_SIZE - start.length();
      char[] spaces = new char[spaceLength];
      Arrays.fill(spaces, '.');

      return start.concat(new String(spaces));
    }
  }

  /**
   * Throws an exception if the test condition is not met.
   */
  protected final void assertTrue(String intention, boolean test) {
    if (!test) {
      stepDone(FAILURE, "Assertion '" + intention + "' failed.");
    }
  }

  /**
   * Thows an exception if expected is not equal to actual.
   */
  protected final void assertEquals(String description,
                                    Object expected,
                                    Object actual) {
    if (!expected.equals(actual)) {
      stepDone(FAILURE, "Assertion '" + description +
               "' failed, expected: '" + expected +
               "' got: " + actual + "'");
    }
  }


  // ----- COMMAND LINE PARSING METHODS -----

  /**
   * process command line args
   */
  protected static void parseArgs(String args[]) {
    // process command line arguments

    for (String arg : args) {
      if (arg.equals("-help")) {
        System.out.println("Usage: DistCommonAPITest [-port p] [-protocol (rmi|wire)] [-bootstrap host[:port]] [-help]");
        System.exit(1);
      }
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-globalNodes") && i+1 < args.length) {
        int p = Integer.parseInt(args[i+1]);
        if (p > 0) NUM_GLOBAL_NODES = p;
        break;
      }
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-organizations") && i+1 < args.length) {
        int p = Integer.parseInt(args[i+1]);
        if (p > 0) NUM_ORGANIZATIONS = p;
        break;
      }
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-internalNodes") && i+1 < args.length) {
        int p = Integer.parseInt(args[i+1]);
        if (p > 0) NUM_INTERNAL_NODES = p;
        break;
      }
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-port") && i+1 < args.length) {
        int p = Integer.parseInt(args[i+1]);
        if (p > 0) PORT = p;
        break;
      }
    }

    BOOTSTRAP_PORT = PORT;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-bootstrap") && i+1 < args.length) {
        String str = args[i+1];
        int index = str.indexOf(':');
        if (index == -1) {
          BOOTSTRAP_HOST = str;
          BOOTSTRAP_PORT = PORT;
        } else {
          BOOTSTRAP_HOST = str.substring(0, index);
          BOOTSTRAP_PORT = Integer.parseInt(str.substring(index + 1));
          if (BOOTSTRAP_PORT <= 0) BOOTSTRAP_PORT = PORT;
        }
        break;
      }
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-protocol") && i+1 < args.length) {
        String s = args[i+1];

          if (s.equalsIgnoreCase("socket"))
          PROTOCOL = DistPastryNodeFactory.PROTOCOL_SOCKET;
        else if (s.equalsIgnoreCase("direct"))
          PROTOCOL = PROTOCOL_DIRECT;
        else
          System.out.println("ERROR: Unsupported protocol: " + s);

        break;
      }
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-simulator") && i+1 < args.length) {
        String s = args[i+1];

        if (s.equalsIgnoreCase("sphere"))
          SIMULATOR = SIMULATOR_SPHERE;
        else if (s.equalsIgnoreCase("euclidean"))
          SIMULATOR = SIMULATOR_EUCLIDEAN;
        else
          System.out.println("ERROR: Unsupported simulator: " + s);

        break;
      }
    }
  }

  public static void main(String[] args) throws IOException {
    parseArgs(args);
    Environment env;
    if (PROTOCOL == PROTOCOL_DIRECT) {
      env = Environment.directEnvironment();
    } else {
      env = new Environment();
    }
    MultiringRegrTest test = new MultiringRegrTest(env);
    int status = 1;
    try {
      test.start();
      if (test.getFailures().isEmpty()) status = 0;
    } catch (AssertionError e) {
      e.printStackTrace();
    } finally {
      env.destroy();
    }
    System.exit(status);
  }
}
