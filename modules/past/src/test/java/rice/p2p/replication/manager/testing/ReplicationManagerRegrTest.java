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
package rice.p2p.replication.manager.testing;

import java.io.IOException;

import java.util.concurrent.CompletableFuture;

import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.testing.CommonAPITest;
import rice.p2p.replication.manager.ReplicaFetchCoordinatorImpl;
import rice.p2p.replication.manager.ReplicationManagerClient;

/**
 * Provides regression testing for the replication manager service using distributed
 * nodes.
 *
 * @version $Id: ReplicationManagerRegrTest.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public class ReplicationManagerRegrTest extends CommonAPITest {

  /**
   * The replication factor to use
   */
  public static final int REPLICATION_FACTOR = 3;

  /** How long to wait for the replica count to settle before the assertion decides. */
  private static final long REPLICA_TIMEOUT_MILLIS = 60_000;

  /**
   * the instance name to use
   */
  public static String INSTANCE = "ReplicationRegrTest";

  /**
   * the replication impls in the ring
   */
  protected ReplicaFetchCoordinatorImpl[] replications;

  /**
   * The clients
   */
  protected TestReplicationManagerClient[] clients;

  /**
   * Constructor which sets up all local variables
   */
  public ReplicationManagerRegrTest(Environment env) throws IOException {
    super(env);
    replications = new ReplicaFetchCoordinatorImpl[NUM_NODES];
    clients = new TestReplicationManagerClient[NUM_NODES];
  }


  /**
   * Usage: ReplicationRegrTest [-port p] [-bootstrap host[:port]] [-nodes n] [-protocol (rmi|wire)]
   * [-help]
   */
  public static void main(String args[]) throws IOException {
    Environment env = parseArgs(args);
    ReplicationManagerRegrTest test = new ReplicationManagerRegrTest(env);
    test.start();
    env.destroy();
  }

  /**
   * Method which should process the given newly-created node
   *
   * @param node The newly created node
   * @param nodeIndex The number of this node
   */
  @Override
  protected void processNode(int nodeIndex, Node node) {
    clients[nodeIndex] = new TestReplicationManagerClient(node);
    replications[nodeIndex] = new ReplicaFetchCoordinatorImpl(node, clients[nodeIndex], REPLICATION_FACTOR, INSTANCE);
  }

  /**
   * Method which should run the test - this is called once all of the nodes have been created and
   * are ready.
   */
  @Override
  protected void runTest() {
    for (int i=0; i<NUM_NODES; i++)
      simulate();

    testBasic();
    testOverload();
    testStress();
    testMaintenance();
  }

  /*
   *  ---------- Test methods and classes ----------
   */

  /**
    * Tests basic functionality
   */
  public void testBasic() {
    int nodeIndex = environment.getRandomSource().nextInt(NUM_NODES);
    Id id = nodes[nodeIndex].getId();

    IdRange fullRange = FACTORY.buildIdRange(FACTORY.buildId(new byte[20]), FACTORY.buildId(new byte[20]));

    sectionStart("Testing Basic Functionality");

    stepStart("Inserting Object");

    clients[nodeIndex].insert(id);

    stepDone(SUCCESS);

    stepStart("Initiating Maintenance");

    runMaintenance();

    for (int i=0; i<NUM_NODES; i++)
      simulate();

    int count = awaitReplicas(fullRange, id, REPLICATION_FACTOR + 1);

    assertTrue("Correct number of replicas should be " + (REPLICATION_FACTOR + 1) + " was " + count,
               count == REPLICATION_FACTOR + 1);

    stepDone(SUCCESS);

    sectionDone();
  }

  /**
    * Tests maintenance functionality
   */
  public void testMaintenance() {
    int nodeIndex = environment.getRandomSource().nextInt(NUM_NODES);
    Id id = nodes[nodeIndex].getId();

    IdRange fullRange = FACTORY.buildIdRange(FACTORY.buildId(new byte[20]), FACTORY.buildId(new byte[20]));

    sectionStart("Testing Maintenance Functionality");

    stepStart("Inserting Object");

    clients[nodeIndex].insert(id);

    stepDone(SUCCESS);

    stepStart("Initiating Maintenance");

    runMaintenance();
    simulate();

    int count = awaitReplicas(fullRange, id, REPLICATION_FACTOR + 1);

    assertTrue("Correct number of replicas should be " + (REPLICATION_FACTOR + 1) + " was " + count,
               count == REPLICATION_FACTOR + 1);

    stepDone(SUCCESS);

    stepStart("Killing Primary Replica");

    kill(nodeIndex);

    stepDone(SUCCESS);

    // wait for notification of failure to propegate
    // No routing involved... but we need to wait long enough for the node
    // to be found faulty.
    waitToRecoverFromKilling(5000);
    waitToRecoverFromKilling(5000);  // 1ce was not enough?  try waiting more?

    stepStart("Initiating Maintenance");

    runMaintenance();
    simulate();

    count = awaitReplicas(fullRange, id, REPLICATION_FACTOR + 2);

    assertTrue("Correct number of replicas should be " + (REPLICATION_FACTOR + 2) + " was " + count,
               count == REPLICATION_FACTOR + 2);

    stepDone(SUCCESS);

    sectionDone();
  }

  /**
    * Tests basic functionality
   */
  public void testOverload() {
    int NUM_TO_INSERT = 16;
    int nodeIndex = environment.getRandomSource().nextInt(NUM_NODES);
    Id id = nodes[nodeIndex].getId();

    IdRange fullRange = FACTORY.buildIdRange(FACTORY.buildId(new byte[20]), FACTORY.buildId(new byte[20]));

    sectionStart("Testing Overload Functionality");

    stepStart("Inserting " + NUM_TO_INSERT + " Objects");

    for (int i=0; i<NUM_TO_INSERT; i++) {
      clients[nodeIndex].insert(addToId(id, i));
      simulate();
    }

    stepDone(SUCCESS);

    stepStart("Initiating Maintenance");

    runMaintenance();
    simulate();

    for (int i=0; i<NUM_TO_INSERT+1; i++) {
      try {
        Thread.sleep(replications[0].FETCH_DELAY);
      } catch (InterruptedException e) {
        System.out.println(e.toString());
      }

      simulate();
    }

    for (int j=0; j<NUM_TO_INSERT; j++) {
      int count = 0;

      Id thisId = addToId(id, j);

      for (int i=0; i<NUM_NODES; i++)  {
        if (clients[i].scan(fullRange).isMemberId(thisId))
          count++;
      }

      assertTrue("Correct number of replicas for "+j+":" + thisId + " should be " + (REPLICATION_FACTOR + 1) + " was " + count,
                 count == REPLICATION_FACTOR + 1);
    }

    stepDone(SUCCESS);

    sectionDone();
  }

  /**
    * Tests basic functionality
   */
  public void testStress() {
    int NUM_TO_INSERT = 45;
    Id[] ids = new Id[NUM_TO_INSERT];
    int nodeIndex = environment.getRandomSource().nextInt(NUM_NODES);
    Id id = nodes[nodeIndex].getId();

    IdRange fullRange = FACTORY.buildIdRange(FACTORY.buildId(new byte[20]), FACTORY.buildId(new byte[20]));

    sectionStart("Testing Stressed Functionality");

    stepStart("Inserting " + NUM_TO_INSERT + " Objects");

    for (int i=0; i<NUM_TO_INSERT; i++) {
      ids[i] = addToId(id, i);
      clients[nodeIndex].insert(ids[i]);
    }

    stepDone(SUCCESS);

    stepStart("Initiating Maintenance");

    runMaintenance();
    simulate();

    try {
      Thread.sleep(25000);
    } catch (InterruptedException e) {
      System.out.println(e.toString());
    }

    simulate();

    for (int j=0; j<NUM_TO_INSERT; j++) {
      int count = 0;

      Id thisId = ids[j];

      for (int i=0; i<NUM_NODES; i++)  {
        if (clients[i].scan(fullRange).isMemberId(thisId))
          count++;
      }

      assertTrue("Correct number of replicas for " + j + " " + thisId + " should be " + (REPLICATION_FACTOR + 1) + " was " + count,
                 count == REPLICATION_FACTOR + 1);
    }

    stepDone(SUCCESS);

    sectionDone();
  }

  /**
   * Counts the nodes holding the id, polling until the expected count is reached or the timeout passes. Replica
   * fetches complete asynchronously, so a fixed pause is flaky when the machine is loaded.
   */
  private int awaitReplicas(IdRange fullRange, Id id, int expected) {
    long deadline = System.currentTimeMillis() + REPLICA_TIMEOUT_MILLIS;
    int count = countReplicas(fullRange, id);
    while (count != expected && System.currentTimeMillis() < deadline) {
      simulate();
      count = countReplicas(fullRange, id);
    }
    return count;
  }

  private int countReplicas(IdRange fullRange, Id id) {
    int count = 0;
    for (int i = 0; i < NUM_NODES; i++) {
      if (clients[i].scan(fullRange).isMemberId(id)) {
        count++;
      }
    }
    return count;
  }

  public void printValsForRange(IdRange range) {
    for (int i=0; i<NUM_NODES; i++)  {
      System.out.println(i+" "+clients[i]+":"+clients[i].scan(range));
    }
  }

  public void runMaintenance() {
    for ( int i=0; i<NUM_NODES; i++) {
      final int j = i;
      environment.getSelectorManager().invoke(() -> replications[j].getReplication().replicate());
    }

    simulate();
  }

  private Id addToId(Id id, int delta) {
    byte[] bytes = id.toByteArray();
    bytes[0] = (byte) (bytes[0] + delta);

    return FACTORY.buildId(bytes);
  }

  /**
   * @version $Id: ReplicationManagerRegrTest.java 4654 2009-01-08 16:33:07Z jeffh $
   * @author amislove
   */
  protected static class TestReplicationManagerClient implements ReplicationManagerClient {

    public Node node;

    public IdSet set;

    public TestReplicationManagerClient(Node node) {
      this.set = node.getIdFactory().buildIdSet();
      this.node = node;
    }

    @Override
    public CompletableFuture<Boolean> fetch(Id id, NodeHandle hint) {
      set.addId(id);
      return CompletableFuture.completedFuture(Boolean.TRUE);
    }

    @Override
    public CompletableFuture<Boolean> remove(Id id) {
      set.removeId(id);
      return CompletableFuture.completedFuture(Boolean.TRUE);
    }

    @Override
    public IdSet scan(IdRange range) {
      return set.subSet(range);
    }

    public void insert(Id id) {
      set.addId(id);
    }

    @Override
    public boolean exists(Id id) {
      return set.isMemberId(id);
    }

    @Override
    public CompletableFuture<Boolean> existsInOverlay(Id id) {
      // TODO: existsInOverlay() and reInsert() are not tested yet
      return CompletableFuture.completedFuture(Boolean.TRUE);
    }

    @Override
    public CompletableFuture<Boolean> reInsert(Id id) {
      // TODO: existsInOverlay() and reInsert() are not tested yet
      return CompletableFuture.completedFuture(Boolean.TRUE);
    }

    @Override
    public String toString() {
      String name = "TRMC:"+node;
      return name;
    }
  }
}
