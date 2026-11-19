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
package rice.pastry.direct.proximitygenerators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.pastry.direct.ProximityGenerator;
import rice.pastry.direct.SimulatedNodePosition;

public class GenericProximityGenerator implements ProximityGenerator {
  private RandomSource random;

  private final Logger logger;

  float MIN_DIST = 2.0f;

  // This stores the matrix stored in millis
  private float distance[][];

  /**
   * The number of stubs.
   */
  int maxOverlaySize = 2000;

  // This keeps track of the indices that have already been assigned
  // index -> ctr
  private final HashMap<Integer, Integer> assignedIndices = new HashMap<Integer, Integer>();

  private Path inFile_Matrix;

  public GenericProximityGenerator(Environment env, Path inFile) throws IOException {
    logger = env.getLogManager().getLogger(GenericProximityGenerator.class, null);
    GenericProximityConfig config = GenericProximityConfig.fromParameters(env.getParameters());
    maxOverlaySize = config.maxOverlaySize();
    MIN_DIST = config.minDelay();
    float delayFactor = config.delayFactor();

    inFile_Matrix = inFile;
    if (inFile_Matrix == null) {
      inFile_Matrix = Path.of(config.matrixFile());
    }

    setNodesPerStub(config.nodesPerStub());

    readOverlayMatrix(delayFactor);
  }



  @Override
  public SimulatedNodePosition generateNodeRecord() {
    return new GNNodeRecord();
  }

  public class GNNodeRecord implements SimulatedNodePosition {
    private int index; // index in the symmetric inter-host latency matrix

    public GNNodeRecord() {
      if (numNodes >= maxOverlaySize*nodesPerStub) throw new RuntimeException("No more nodes int he network.");
      index = random.nextInt(maxOverlaySize);
      while (stubIsFull(index)) {
        index = random.nextInt(maxOverlaySize);
      }

      incrementStub(index);
    }

    @Override
    public float proximity(SimulatedNodePosition that) {
      return Math.round(networkDelay(that)+that.networkDelay(this));
    }

    @Override
    public float networkDelay(SimulatedNodePosition that) {
      GNNodeRecord nr = (GNNodeRecord)that;
      float res = distance[index][nr.index];
      if (res < 0)
        return Float.MAX_VALUE;

      if ((res < MIN_DIST) && !this.equals(that)) return MIN_DIST;

      return res;
    }
  }

  int nodesPerStub = 1;

  public void setNodesPerStub(int numPerStub) {
    this.nodesPerStub = numPerStub;
  }

  public void readOverlayMatrix(float delayFactor) throws IOException {
    InputStream fr;
    try {
      fr = Files.newInputStream(inFile_Matrix);
    } catch (IOException e) {
      throw new IOException("ERROR: The required inter-host distance matrix for Generic Network not found:"+inFile_Matrix.toAbsolutePath(), e);
    }

    int lineCount = 0;
    String line = null;
    try (BufferedReader in = new BufferedReader(new InputStreamReader(fr, StandardCharsets.UTF_8))) {
      while ((line = in.readLine()) != null) {
        String[] words;
        words = line.split("[ \t]+");
        if (distance == null) {
          if (words.length < maxOverlaySize)
            maxOverlaySize = words.length;
          distance = new float[maxOverlaySize][maxOverlaySize];
        }
        int nodeCount = 0;
        for (int i = 0; i < maxOverlaySize; i++) {
          if (words[i].length() > 0) {
            distance[lineCount][nodeCount] = delayFactor*Float.parseFloat(words[i]);
            nodeCount++;
            if (nodeCount == maxOverlaySize) break;
          }
        }
        lineCount++;
        if (lineCount == maxOverlaySize) break;
      }
      final int size = lineCount;
      logger.info("Size of Generic Network matrix= {}", size);
    } catch (IOException e) {
      logger.warn("Exception reading the Generic Network matrix", e);
    }
  }

  private boolean stubIsFull(int index) {
    if (!assignedIndices.containsKey(index)) return false;
    return assignedIndices.get(index)>=nodesPerStub;
  }

  /**
   * The number of nodes assigned.
   */
  int numNodes = 0;

  private void incrementStub(int index) {
    numNodes++;

    int val = 0;
    if (assignedIndices.containsKey(index)) {
      val = assignedIndices.get(index);
    }
    assignedIndices.put(index, val+1);
  }


  @Override
  public void setRandom(RandomSource random) {
    this.random = random;
  }

}
