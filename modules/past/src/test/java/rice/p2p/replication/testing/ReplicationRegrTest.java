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
package rice.p2p.replication.testing;

import java.io.IOException;
import java.util.Iterator;

import rice.Continuation.ListenerContinuation;
import rice.Futures;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.testing.CommonAPITest;
import rice.p2p.replication.ReplicationClient;
import rice.p2p.replication.ReplicationImpl;
import rice.persistence.MemoryStorage;

/**
 * Provides regression testing for the replication service using distributed
 * nodes.
 *
 * @version $Id: ReplicationRegrTest.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public class ReplicationRegrTest extends CommonAPITest {

  /**
   * The replication factor to use
   */
  public static final int REPLICATION_FACTOR = 3;

  /**
   * the instance name to use
   */
  public static String INSTANCE = "ReplicationRegrTest";

  /**
   * the replication impls in the ring
   */
  protected ReplicationImpl[] replications;

  /**
   * The clients
   */
  protected TestReplicationClient[] clients;

  /**
   * Constructor which sets up all local variables
   */
  public ReplicationRegrTest(Environment env) throws IOException {
    super(env);
    replications = new ReplicationImpl[NUM_NODES];
    clients = new TestReplicationClient[NUM_NODES];
  }


  /**
   * Usage: ReplicationRegrTest [-port p] [-bootstrap host[:port]] [-nodes n] [-protocol (rmi|wire)]
   * [-help]
   */
  public static void main(String args[]) throws IOException {
    Environment env = parseArgs(args);
    ReplicationRegrTest test = new ReplicationRegrTest(env);
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
    clients[nodeIndex] = new TestReplicationClient(node);
    replications[nodeIndex] = new ReplicationImpl(node, clients[nodeIndex], REPLICATION_FACTOR, INSTANCE);
  }

  /**
   * Method which should run the test - this is called once all of the nodes have been created and
   * are ready.
   */
  @Override
  protected void runTest() {
    testBasic();
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
    simulate();

    int count = awaitReplicas(id, fullRange, REPLICATION_FACTOR + 1);

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

    sectionStart("Testing Basic Functionality");

    stepStart("Inserting Object");

    clients[nodeIndex].insert(id);

    stepDone(SUCCESS);

    stepStart("Initiating Maintenance");

    runMaintenance();
    simulate();

    int count = awaitReplicas(id, fullRange, REPLICATION_FACTOR + 1);

    assertTrue("Correct number of replicas should be " + (REPLICATION_FACTOR + 1) + " was " + count,
               count == REPLICATION_FACTOR + 1);

    stepDone(SUCCESS);

    stepStart("Killing Primary Replica");

    kill(nodeIndex);

    // wait for notification of failure to propegate
    // No routing involved... but we need to wait long enough for the node
    // to be found faulty.
    waitToRecoverFromKilling(5000);

    stepDone(SUCCESS);

    stepStart("Initiating Maintenance");

    runMaintenance();
    simulate();

    count = awaitReplicas(id, fullRange, REPLICATION_FACTOR + 2);

    assertTrue("Correct number of replicas should be " + (REPLICATION_FACTOR + 2) + " was " + count,
               count == REPLICATION_FACTOR + 2);

    stepDone(SUCCESS);

    sectionDone();
  }

  /**
   * Counts the nodes holding the id, simulating until the count is as expected or about 10 s passed: the
   * replication runs on the processing thread, so a fixed single wait made this test flaky under load.
   */
  protected int awaitReplicas(Id id, IdRange fullRange, int expected) {
    int count = 0;
    for (int round = 0; round < 20; round++) {
      count = 0;
      for (int i=0; i<NUM_NODES; i++)  {
        if (clients[i].scan(fullRange).isMemberId(id))
          count++;
      }
      if (count == expected) return count;
      simulate();
    }
    return count;
  }

  public void runMaintenance() {
    for (int i=0; i<NUM_NODES; i++) {
      replications[i].replicate();
    }

    simulate();
  }

  /**
   * @version $Id: ReplicationRegrTest.java 4654 2009-01-08 16:33:07Z jeffh $
   * @author amislove
   */
  protected class TestReplicationClient implements ReplicationClient {

    public MemoryStorage storage;

    public Node node;

    public TestReplicationClient(Node node) {
      this.storage = new MemoryStorage(FACTORY);
      this.node = node;
    }

    /**
     * This upcall is invoked to notify the application that is should
     * fetch the cooresponding keys in this set, since the node is now
     * responsible for these keys also.
     *
     * @param keySet set containing the keys that needs to be fetched
     */
    @Override
    public void fetch(IdSet keySet, NodeHandle hint) {
      Iterator<Id> idIterator = keySet.getIterator();

      while (idIterator.hasNext()) {
        Id next = (Id) idIterator.next();
        Futures.forward(storage.store(next, null, next), new ListenerContinuation<Object, Exception>("Insertion of " + next, environment));
      }
    }

    /**
     * This upcall is to notify the application of the range of keys for
     * which it is responsible. The application might choose to react to
     * call by calling a scan(complement of this range) to the persistance
     * manager and get the keys for which it is not responsible and
     * call delete on the persistance manager for those objects.
     *
     * @param range the range of keys for which the local node is currently
     *              responsible
     */
    @Override
    public void setRange(IdRange range) {
      IdRange notRange = range.getComplementRange();
      IdSet idSet = storage.scan(notRange);

      Iterator<Id> idIterator = idSet.getIterator();

      while (idIterator.hasNext()) {
        Id next = (Id) idIterator.next();
        Futures.forward(storage.unstore(next), new ListenerContinuation<Object, Exception>("Removal of " + next, environment));
      }
    }

    /**
     * This upcall should return the set of keys that the application
     * currently stores in this range. Should return a empty IdSet (not null),
     * in the case that no keys belong to this range.
     *
     * @param range the requested range
     */
    @Override
    public IdSet scan(IdRange range) {
      return storage.scan(range);
    }

    public void insert(Id id) {
      Futures.forward(storage.store(id, null, id), new ListenerContinuation<Object, Exception>("Insertion of id " + id, environment));
    }

    @Override
    public String toString() {
      return "TestRepClient "+node.toString();
    }
  }
}
