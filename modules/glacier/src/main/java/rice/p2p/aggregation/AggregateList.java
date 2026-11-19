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
package rice.p2p.aggregation;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.glacier.VersionKey;

@SuppressWarnings("unchecked")
public class AggregateList {

  private final ConcurrentHashMap<Id, AggregateDescriptor> aggregateList;
  private final String configFileName;
  private final String logFileName;
  private final String label;
  private final IdFactory factory;
  private final PrintStream logFile;
  private final boolean loggingEnabled;
  private Id rootKey;
  private final boolean wasReadOK;
  private long nextSerial;
  private final Environment environment;
  private final Logger logger;

  public AggregateList(String configFileName, String label, IdFactory factory, boolean loggingEnabled, String instance, Environment env) throws IOException {
    this.configFileName = configFileName;
    this.aggregateList = new ConcurrentHashMap<>();
    this.factory = factory;
    this.label = label;
    this.rootKey = null;
    this.nextSerial = 0;
    this.environment = env;
    this.logger = environment.getLogManager().getLogger(AggregateList.class, instance);
    this.wasReadOK = readFromDisk();
    this.loggingEnabled = loggingEnabled;
    this.logFileName = configFileName + ".log";

    if (loggingEnabled) {
      recoverLog();
      this.logFile = new PrintStream(Files.newOutputStream(Path.of(logFileName), CREATE, APPEND));
    } else {
      this.logFile = null;
    }
  }

  public boolean wasReadSuccessfully() {
    return wasReadOK;
  }

  public Enumeration<AggregateDescriptor> elements() {
    return Collections.enumeration(aggregateList.values());
  }

  public void recoverLog() {
    long expectedSerial = -1;
    int entriesReplayed = 0;

    try (BufferedReader logFile = new BufferedReader(new InputStreamReader(Files.newInputStream(Path.of(logFileName)), StandardCharsets.UTF_8))) {

      while (true) {
        String line = readLineSkipComments(logFile);
        if (line == null)
          break;

        if (!line.startsWith("$") || !line.endsWith("@"))
          throw new AggregationException("Malformed log entry (expected $...@): "+line);

        if ((line.lastIndexOf('$') != 0) || (line.indexOf('@') != line.length()-1))
          throw new AggregationException("Overlapping log entries: "+line);

        @SuppressWarnings("StringSplitter") // parses the fixed format writeToDisk/appendLog write, no trailing empty fields
        String[] parts = line.split("\\|");

        long thisSerial = Long.parseLong(parts[1]);
        if ((expectedSerial >= 0) && (thisSerial != expectedSerial))
          throw new AggregationException("Malformed log entry (expected serial #"+expectedSerial+"): "+line);

        expectedSerial = thisSerial + 1;

        if (thisSerial > nextSerial)
          throw new AggregationException("Entries "+nextSerial+".."+(thisSerial-1)+" missing from log... cannot recover!");

        if (thisSerial == nextSerial) {
          logger.debug("Replaying log entry #{}", thisSerial);
          entriesReplayed ++;

          if (parts[3].equals("setRoot")) {
            if (parts[4].equals("null")) {
              rootKey = null;
              logger.trace("  - rootKey = null");
            } else {
              rootKey = factory.buildIdFromString(parts[4]);
              logger.atDebug().log(() -> "  - rootKey = "+rootKey.toStringFull());
            }

          } else if (parts[3].equals("setAL")) {
            Id adcKey = factory.buildIdFromString(parts[4]);
            long lifetime = Long.parseLong(parts[5]);
            AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.get(adcKey);
            if (aggregate == null)
              throw new AggregationException("Cannot find aggregate ("+adcKey.toStringFull()+": "+line);
            aggregate.setCurrentLifetime(lifetime);
            logger.atTrace().log(() -> "  - lifetime="+lifetime+" in ADC "+adcKey.toStringFull());

          } else if (parts[3].equals("setOCL")) {
            Id adcKey = factory.buildIdFromString(parts[4]);
            int index = Integer.parseInt(parts[5]);
            long lifetime = Long.parseLong(parts[6]);
            AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.get(adcKey);
            if (aggregate == null)
              throw new AggregationException("Cannot find aggregate ("+adcKey.toStringFull()+": "+line);
            if (aggregate.getObjects().length <= index)
              throw new AggregationException("Object index mismatch ("+index+"/"+aggregate.getObjects().length+"): "+line);
            aggregate.getObjects()[index].setCurrentLifetime(lifetime);
            logger.atTrace().log(() -> "  - currentLifetime="+lifetime+" in ADC "+adcKey.toStringFull()+" index "+index);

          } else if (parts[3].equals("setORL")) {
            Id adcKey = factory.buildIdFromString(parts[4]);
            int index = Integer.parseInt(parts[5]);
            long lifetime = Long.parseLong(parts[6]);
            AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.get(adcKey);
            if (aggregate == null)
              throw new AggregationException("Cannot find aggregate ("+adcKey.toStringFull()+": "+line);
            if (aggregate.getObjects().length <= index)
              throw new AggregationException("Object index mismatch ("+index+"/"+aggregate.getObjects().length+"): "+line);
            aggregate.getObjects()[index].setRefreshedLifetime(lifetime);
            logger.atTrace().log(() -> "  - refreshedLifetime="+lifetime+" in ADC "+adcKey.toStringFull()+" index "+index);

          } else if (parts[3].equals("refresh")) {
            Id adcKey = factory.buildIdFromString(parts[4]);
            long lifetime = Long.parseLong(parts[5]);
            AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.get(adcKey);
            if (aggregate == null)
              throw new AggregationException("Cannot find aggregate ("+adcKey.toStringFull()+": "+line);

            aggregate.setCurrentLifetime(lifetime);
            for (int i=0; i<aggregate.getObjects().length; i++)
              aggregate.getObjects()[i].setCurrentLifetime(aggregate.getObjects()[i].getRefreshedLifetime());
            logger.atTrace().log(() -> " - refresh="+lifetime+" in ADC "+adcKey.toStringFull());

          } else if (parts[3].equals("removeAggregate")) {
            Id adcKey = factory.buildIdFromString(parts[4]);
            AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.get(adcKey);
            if (aggregate == null)
              throw new AggregationException("Cannot find aggregate ("+adcKey.toStringFull()+": "+line);

            removeAggregateDescriptor(aggregate, false);
            logger.atTrace().log(() -> " - remove ADC "+adcKey.toStringFull());

          } else if (parts[3].equals("addAggregate")) {
            Id adcKey = factory.buildIdFromString(parts[4]);
            AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.get(adcKey);
            if (aggregate != null)
              throw new AggregationException("Aggregate already present ("+adcKey.toStringFull()+": "+line);

            aggregate = readAggregate(logFile, adcKey);
            addAggregateDescriptor(aggregate, false);
            logger.atTrace().log(() -> " - add ADC "+adcKey.toStringFull());
          } else {
            throw new AggregationException("Unknown command ("+parts[3]+"): "+line);
          }

          nextSerial ++;
        } else {
          /* skip addAggregate if necessary */

          if (parts[3].equals("addAggregate")) {
            readAggregate(logFile, factory.buildIdFromString(parts[4]));
          }
        }
      }

    } catch (FileSystemException fnfe) {
      // the log could not be opened, as FileReader reported with a FileNotFoundException
      logger.warn("No aggregate log found; using configuration file only");
    } catch (Exception e) {
      // a half-replayed log leaves the list inconsistent, so refuse to continue
      throw new IllegalStateException("Cannot recover aggregate log "+logFileName, e);
    }

    if (entriesReplayed > 0) {
      writeToDisk();
      logger.warn("{} entries replayed from aggregate log", entriesReplayed);
    }
  }

  public Id getRoot() {
    return rootKey;
  }

  public void logEntry(String entry) {
    if (loggingEnabled) {
      if (logFile != null) {
        logFile.println("$|"+ nextSerial+++"|"+environment.getTimeSource().currentTimeMillis()+"|"+entry+"|@");
        logFile.flush();
      } else {
        logger.warn("Aggregation cannot write to log: {}", entry);
      }
    }
  }

  public void setRoot(Id root) {
    rootKey = root;
    logEntry("setRoot|"+((root == null) ? "null" : root.toStringFull()));
  }

  public boolean isEmpty() {
    return aggregateList.isEmpty();
  }

  public void clear() {
    aggregateList.clear();
    rootKey = null;
  }

  public AggregateDescriptor getAggregateDescriptor(Id id) {
    return (AggregateDescriptor) aggregateList.get(id);
  }

  public void setAggregateLifetime(AggregateDescriptor aggregate, long lifetime) {
    logEntry("setAL|"+aggregate.getKey().toStringFull()+"|"+lifetime);
    aggregate.setCurrentLifetime(lifetime);
  }

  public void setObjectCurrentLifetime(AggregateDescriptor aggregate, int index, long lifetime) {
    logEntry("setOCL|"+aggregate.getKey().toStringFull()+"|"+index+"|"+lifetime);
    aggregate.getObjects()[index].setCurrentLifetime(lifetime);
  }

  public void setObjectRefreshedLifetime(AggregateDescriptor aggregate, int index, long lifetime) {
    logEntry("setORL|"+aggregate.getKey().toStringFull()+"|"+index+"|"+lifetime);
    aggregate.getObjects()[index].setRefreshedLifetime(lifetime);
  }

  public void resetMarkers() {
    for (AggregateDescriptor aggr : aggregateList.values())
      aggr.setMarker(false);
  }

  public void refreshAggregate(AggregateDescriptor aggregate, long lifetime) {
    logEntry("refresh|"+aggregate.getKey().toStringFull()+"|"+lifetime);
    aggregate.setCurrentLifetime(lifetime);
    for (int i=0; i<aggregate.getObjects().length; i++)
      aggregate.getObjects()[i].setCurrentLifetime(aggregate.getObjects()[i].getRefreshedLifetime());
  }

  public void addAggregateDescriptor(AggregateDescriptor aggr) {
    addAggregateDescriptor(aggr, true);
  }

  private void addAggregateDescriptor(AggregateDescriptor aggr, boolean logThis) {
    if ((logFile != null) && logThis && loggingEnabled) {
      logEntry("addAggregate|"+aggr.getKey().toStringFull());
      writeAggregate(logFile, aggr);
      logFile.flush();
    }

    aggregateList.put(aggr.getKey(), aggr);

    for (int i=0; i<aggr.getObjects().length; i++) {
      aggregateList.put(new VersionKey(aggr.getObjects()[i].getKey(), aggr.getObjects()[i].getVersion()), aggr);
      AggregateDescriptor prevDesc = (AggregateDescriptor) aggregateList.get(aggr.getObjects()[i].getKey());
      int objDescIndex = (prevDesc == null) ? -1 : prevDesc.lookupNewest(aggr.getObjects()[i].getKey());
      if ((objDescIndex < 0) || (prevDesc.getObjects()[objDescIndex].getVersion() <= aggr.getObjects()[i].getVersion())) {
        aggregateList.put(aggr.getObjects()[i].getKey(), aggr);
      }
    }

    for (int i=0; i<aggr.getPointers().length; i++) {
      AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.get(aggr.getPointers()[i]);
      if (aggregate != null)
        aggregate.addReference();
    }
  }

  public void removeAggregateDescriptor(AggregateDescriptor aggr) {
    removeAggregateDescriptor(aggr, true);
  }

  private void removeAggregateDescriptor(AggregateDescriptor aggr, boolean logThis) {
    if (logThis)
      logEntry("removeAggregate|"+aggr.getKey().toStringFull());

    aggregateList.remove(aggr.getKey());

    for (int i=0; i<aggr.getObjects().length; i++) {
      VersionKey vkey = new VersionKey(aggr.getObjects()[i].getKey(), aggr.getObjects()[i].getVersion());
      AggregateDescriptor prevDesc1 = (AggregateDescriptor) aggregateList.get(vkey);
      if ((prevDesc1 != null) && prevDesc1.getKey().equals(aggr.getKey()))
        aggregateList.remove(vkey);
      AggregateDescriptor prevDesc2 = (AggregateDescriptor) aggregateList.get(aggr.getObjects()[i].getKey());
      if ((prevDesc2 != null) && prevDesc2.getKey().equals(aggr.getKey()))
        aggregateList.remove(aggr.getObjects()[i].getKey());
    }

    if (aggregateList.containsValue(aggr))
      logger.atWarn().log(() -> "Removal from aggregate list incomplete: "+aggr.getKey().toStringFull());
  }

  public void recalculateReferenceCounts(Id[] excludes) {
    for (AggregateDescriptor aggr : aggregateList.values()) {
      aggr.resetReferenceCount();
      aggr.setMarker(false);

      /* References from excluded aggregates don't count */

      if (excludes != null)
        for (int i=0; i<excludes.length; i++)
          if (excludes[i].equals(aggr.getKey()))
            aggr.setMarker(true);
    }

    for (AggregateDescriptor aggr : aggregateList.values()) {
      if (!aggr.isMarker()) {
        aggr.setMarker(true);
        for (int i=0; i<aggr.getPointers().length; i++) {
          AggregateDescriptor aggregate = (AggregateDescriptor) aggregateList.get(aggr.getPointers()[i]);
          if (aggregate != null)
            aggregate.addReference();
        }
      }
    }
  }

  private String readLineSkipComments(BufferedReader reader) throws IOException {
    while (true) {
      String line = reader.readLine();
      if ((line != null) && ((line.length() == 0) || (line.charAt(0) == '#')))
        continue;
      return line;
    }
  }

  @SuppressWarnings("StringSplitter") // parses the fixed format writeToDisk/appendLog write, no trailing empty fields
  private AggregateDescriptor readAggregate(BufferedReader reader, Id aggrKey) throws AggregationException, IOException {
    String[] expiresS = readLineSkipComments(reader).split("=");
    if (!expiresS[0].equals("expires"))
      throw new AggregationException("Cannot find expiration date: "+expiresS[0]);
    long expires = Long.parseLong(expiresS[1]);

    String[] objectNumS = readLineSkipComments(reader).split("=");
    if (!objectNumS[0].equals("objects"))
      throw new AggregationException("Cannot find number of objects: "+objectNumS[0]);
    int numObjects = Integer.parseInt(objectNumS[1]);

    ObjectDescriptor[] objects = new ObjectDescriptor[numObjects];
    for (int i=0; i<numObjects; i++) {
      String[] objS = readLineSkipComments(reader).split("=");
      String[] objArgS = objS[1].split(";");
      String[] objIdS = objArgS[0].split("v");
      objects[i] = new ObjectDescriptor(
        factory.buildIdFromString(objIdS[0]),
        Long.parseLong(objIdS[1]),
        Long.parseLong(objArgS[1]),
        Long.parseLong(objArgS[2]),
        Integer.parseInt(objArgS[3])
      );
    }

    String[] pointerNumS = readLineSkipComments(reader).split("=");
    if (!pointerNumS[0].equals("pointers"))
      throw new AggregationException("Cannot find number of pointers: "+pointerNumS[0]);
    int numPointers = Integer.parseInt(pointerNumS[1]);

    Id[] pointers = new Id[numPointers];
    for (int i=0; i<numPointers; i++) {
      String[] ptrS = readLineSkipComments(reader).split("=");
      pointers[i] = factory.buildIdFromString(ptrS[1]);
    }

    return new AggregateDescriptor(aggrKey, expires, objects, pointers);
  }

  @SuppressWarnings("StringSplitter") // parses the fixed format writeToDisk/appendLog write, no trailing empty fields
  public boolean readFromDisk() {
    rootKey = null;
    aggregateList.clear();

    String fileName;
    if (Files.exists(Path.of(configFileName)))
      fileName = configFileName;
    else if (Files.exists(Path.of(configFileName + ".new")))
      fileName = configFileName + ".new";
    else {
      return false;
    }

    BufferedReader configFile = null;
    boolean readSuccessful = false;

    try {
      configFile = new BufferedReader(new InputStreamReader(Files.newInputStream(Path.of(fileName)), StandardCharsets.UTF_8));

      String[] root = readLineSkipComments(configFile).split("=");
      if (root[0].equals("nextid")) {
        nextSerial = Long.parseLong(root[1]);
        root = readLineSkipComments(configFile).split("=");
      }
      if (!root[0].equals("root"))
        throw new AggregationException("Cannot read root key: "+root[0]);
      rootKey = factory.buildIdFromString(root[1]);

      while (true) {
        String aggrKeyLine = readLineSkipComments(configFile);
        if (aggrKeyLine == null) {
          readSuccessful = true;
          break;
        }

        String[] aggrKeyS = aggrKeyLine.split("\\[|\\]");
        Id aggrKey = factory.buildIdFromString(aggrKeyS[1]);
        AggregateDescriptor aggregate = readAggregate(configFile, aggrKey);

        addAggregateDescriptor(aggregate, false);
      }
    } catch (Exception e) {
      logger.warn("Cannot read configuration file: {} (e={})", configFileName, e, e);
    }

    if (configFile != null) {
      try {
        configFile.close();
      } catch (IOException ignored) {
        // the contents are already read, a failed close loses nothing
      }
    }

    if (!readSuccessful) {
      rootKey = null;
      aggregateList.clear();
    }

    return readSuccessful;
  }

  private void writeAggregate(PrintStream stream, AggregateDescriptor aggregate) {
    stream.println("expires=" + aggregate.getCurrentLifetime());
    stream.println("objects=" + aggregate.getObjects().length);
    for (int i=0; i<aggregate.getObjects().length; i++)
      stream.println("obj"+i+"="+
        aggregate.getObjects()[i].getKey().toStringFull()+"v"+
        aggregate.getObjects()[i].getVersion()+";"+
        aggregate.getObjects()[i].getCurrentLifetime()+";"+
        aggregate.getObjects()[i].getRefreshedLifetime()+";"+
        aggregate.getObjects()[i].getSize()
      );
    stream.println("pointers=" + aggregate.getPointers().length);
    for (int i=0; i<aggregate.getPointers().length; i++)
      stream.println("ptr"+i+"="+aggregate.getPointers()[i].toStringFull());
  }

  public void writeToDisk() {
    if (rootKey == null)
      return;

    try {
      PrintStream configFile = new PrintStream(Files.newOutputStream(Path.of(configFileName + ".new")));
      resetMarkers();
      configFile.println("# Aggregate list at " + label + " (" + DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.systemDefault())) + ")");
      configFile.println();
      configFile.println("nextid="+nextSerial);
      configFile.println("root="+rootKey.toStringFull());
      configFile.println();

      for (AggregateDescriptor aggr : aggregateList.values()) {
        if (!aggr.isMarker()) {
          configFile.println("["+aggr.getKey().toStringFull()+"]");
          writeAggregate(configFile, aggr);
          configFile.println("");

          aggr.setMarker(true);
        }
      }

      configFile.close();
      // as with delete and renameTo, a failed step is ignored: readFromDisk falls back to the .new file
      try {
        Files.deleteIfExists(Path.of(configFileName));
      } catch (IOException ignored) { /* readFromDisk falls back to the .new file */ }
      try {
        Files.move(Path.of(configFileName + ".new"), Path.of(configFileName), REPLACE_EXISTING);
      } catch (IOException ignored) { /* readFromDisk falls back to the .new file */ }
    } catch (IOException ioException) {
      logger.warn("AggregationImpl cannot write to its aggregate list: {} ({})", configFileName, ioException, ioException);
    }
  }

  public Id[] getSomePointers(int referenceThreshold, int max, Id[] excludes) {
    if (rootKey == null)
      return new Id[] {};

    if (excludes != null)
      recalculateReferenceCounts(excludes);

    List<Id> pointers = new ArrayList<Id>();
    Enumeration<AggregateDescriptor> enumeration = elements();

    resetMarkers();
    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
      if (!aggr.isMarker()) {
        aggr.setMarker(true);

        boolean isExcluded = false;
        if (excludes != null)
          for (int i=0; i<excludes.length; i++)
            if (excludes[i].equals(aggr.getKey()))
              isExcluded = true;

        if ((aggr.getReferenceCount() < referenceThreshold) && (pointers.size() < max) && !isExcluded)
          pointers.add(aggr.getKey());
      }
    }

    return (Id[]) pointers.toArray(new Id[] {});
  }

  public AggregationStatistics getStatistics(long granularity, long range, int nominalReferenceCount) {
    final int maxHistoIndex = (int)(range/granularity);
    final long now = environment.getTimeSource().currentTimeMillis();
    AggregationStatistics stats = new AggregationStatistics(1+maxHistoIndex, granularity, environment);
    Enumeration<AggregateDescriptor> enumeration = elements();

    recalculateReferenceCounts(null);
    resetMarkers();

    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
      if (!aggr.isMarker()) {
        aggr.setMarker(true);

        int aggrPos = (int)((aggr.getCurrentLifetime() - now) / granularity);
        if (aggrPos < 0)
          aggrPos = 0;
        if (aggrPos > maxHistoIndex)
          aggrPos = maxHistoIndex;
        stats.addAggregate(aggr.getObjects().length, aggr.getReferenceCount(), nominalReferenceCount, aggrPos);

        for (int i=0; i<aggr.getObjects().length; i++) {
          int objPos = (int)((aggr.getObjects()[i].getRefreshedLifetime() - now) / granularity);
          if (objPos < 0)
            objPos = 0;
          if (objPos > maxHistoIndex)
            objPos = maxHistoIndex;
          stats.addObject(aggr.getObjects()[i].getSize(), aggr.getObjects()[i].isAliveAt(now), objPos);
        }
      }
    }

    return stats;
  }
}
