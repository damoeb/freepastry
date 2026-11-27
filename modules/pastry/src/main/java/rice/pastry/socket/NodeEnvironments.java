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
package rice.pastry.socket;

import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.logging.Slf4jLogManager;
import rice.environment.processing.Processor;
import rice.environment.processing.simple.SimpleProcessor;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.pastry.Id;
import rice.pastry.transport.PastryFactoryConfig;
import rice.selector.SelectorLoop;

/**
 * Derives the environment of each node a {@link SocketPastryNodeFactory} builds from the factory's root environment.
 * With {@code pastry_factory_multipleNodes} a node gets its own environment: a log manager named after the node, its
 * own random source, and, if configured, its own selector and processor. Otherwise it shares the root environment.
 */
final class NodeEnvironments {
  /** Makes the random source of a node's own environment. */
  @FunctionalInterface
  interface RandomSourceFactory {
    RandomSource create(Environment rootEnvironment, Id nodeId, LogManager lman);
  }

  private final PastryFactoryConfig config;
  private final RandomSourceFactory randomSources;

  NodeEnvironments(PastryFactoryConfig config, RandomSourceFactory randomSources) {
    this.config = config;
    this.randomSources = randomSources;
  }

  /** @return a random source seeded from the root environment's */
  static RandomSource seededFrom(Environment rootEnvironment, LogManager lman) {
    long randSeed = rootEnvironment.getRandomSource().nextLong();
    return new SimpleRandomSource(randSeed, lman);
  }

  Environment cloneEnvironment(Environment rootEnvironment, Id nodeId) {
    Environment result = rootEnvironment;
    if (config.multipleNodes()) {

      // new logManager
      LogManager lman = cloneLogManager(rootEnvironment, nodeId);

      // new selector
      SelectorLoop sman = cloneSelectorManager(rootEnvironment, nodeId, lman);

      // new processor
      Processor proc = cloneProcessor(rootEnvironment, nodeId);

      // new random source
      RandomSource rand = randomSources.create(rootEnvironment, nodeId, lman);

      // build the environment
      result = new Environment(sman, proc, rand, rootEnvironment.getTimeSource(), lman,
          rootEnvironment.getParameters(), rootEnvironment.getExceptionStrategy());

      // gain shared fate with the rootEnvironment
      rootEnvironment.addDestructable(result);
    }
    return result;
  }

  /**
   * Undoes {@link #cloneEnvironment} for a node that was never handed out: unhooks the clone from the root
   * environment and stops the selector and processor the clone created for itself. The root's own selector
   * and processor, which the clone shares unless the factory is configured to give every node its own,
   * stay running, so the clone is not destroyed as a whole.
   */
  @SuppressWarnings("ReferenceEquality") // shared instances are meant
  static void releaseClonedEnvironment(Environment rootEnvironment, Environment cloned) {
    if (cloned == rootEnvironment) return;
    rootEnvironment.removeDestructable(cloned);
    if (cloned.getSelectorManager() != rootEnvironment.getSelectorManager()) {
      cloned.getSelectorManager().destroy();
    }
    if (cloned.getProcessor() != rootEnvironment.getProcessor()) {
      cloned.getProcessor().destroy();
    }
  }

  private static LogManager cloneLogManager(Environment rootEnvironment, Id nodeId) {
    LogManager lman = rootEnvironment.getLogManager();
    if (lman instanceof Slf4jLogManager slm) {
      lman = slm.clone("0x" + nodeId.toStringBare());
    }
    return lman;
  }

  private SelectorLoop cloneSelectorManager(Environment rootEnvironment, Id nodeId, LogManager lman) {
    SelectorLoop sman = rootEnvironment.getSelectorManager();
    if (config.selectorPerNode()) {
      sman = new SelectorLoop(nodeId.toString() + " Selector",
          rootEnvironment.getTimeSource(), lman, rootEnvironment.getRandomSource());
    }
    return sman;
  }

  private Processor cloneProcessor(Environment rootEnvironment, Id nodeId) {
    Processor proc = rootEnvironment.getProcessor();
    if (config.processorPerNode()) {
      proc = new SimpleProcessor(nodeId.toString() + " Processor");
    }
    return proc;
  }
}
