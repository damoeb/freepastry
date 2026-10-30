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

package rice.p2p.commonapi.rawserialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.mpisws.p2p.filetransfer.FileReceipt;
import org.mpisws.p2p.pki.x509.CAToolImpl;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import rice.p2p.glacier.v2.GlacierImpl;
import rice.p2p.multiring.MultiringNode;
import rice.p2p.past.Past;
import rice.p2p.scribe.Scribe;
import rice.pastry.PastryNode;
import rice.persistence.Storage;
import rice.tutorial.lesson1.DistTutorial;

/**
 * Checks the message type-id registries: every enum in the main code that implements
 * {@link MessageTypeId} is one deserializer's scope, so no two of its constants may share an id.
 * It lives in freepastry-tutorials because that module depends on every other one.
 */
class MessageTypeRegistriesTest {

  /** The registries that must exist; the scan below must find at least these. */
  private static final List<String> KNOWN_REGISTRIES = List.of(
      "rice.p2p.scribe.messaging.ScribeMessageType",
      "rice.p2p.past.messaging.PastMessageType",
      "rice.p2p.glacier.v2.messaging.GlacierMessageType",
      "rice.p2p.aggregation.AggregationContentType",
      "rice.p2p.aggregation.AggregationHandleType",
      "org.mpisws.p2p.transport.peerreview.message.PeerReviewMessageType",
      "rice.pastry.join.JoinMessageType",
      "rice.pastry.leafset.LeafSetMessageType",
      "rice.pastry.routing.RouteSetMessageType",
      "rice.pastry.routing.RouterMessageType",
      "rice.pastry.pns.messages.PNSMessageType",
      "rice.pastry.socket.nat.rendezvous.RendezvousMessageType",
      "rice.pastry.socket.nat.probe.ProbeMessageType",
      "rice.p2p.replication.messaging.ReplicationMessageType",
      "rice.p2p.commonapi.IdType",
      "rice.p2p.commonapi.NodeHandleSetType",
      "rice.p2p.splitstream.SplitStreamContentType",
      "rice.p2p.multiring.messaging.MultiringContentType");

  /** One main class of each module; the scan covers the classes of each. */
  private static final List<Class<?>> MODULE_ANCHORS = List.of(
      MessageTypeId.class, TransportLayer.class, FileReceipt.class, PastryNode.class, CAToolImpl.class,
      PeerReview.class, Storage.class, Past.class, Scribe.class, MultiringNode.class, GlacierImpl.class,
      DistTutorial.class);

  @Test
  void everyRegistryHasUniqueIds() throws IOException, URISyntaxException {
    List<Class<?>> registries = findRegistries();
    List<String> names = new ArrayList<>();
    for (Class<?> registry : registries) {
      names.add(registry.getName());
    }
    assertTrue(names.containsAll(KNOWN_REGISTRIES), "missing registries, found " + names);

    List<String> clashes = new ArrayList<>();
    for (Class<?> registry : registries) {
      Object[] constants;
      try {
        constants = registry.getEnumConstants();
      } catch (ExceptionInInitializerError e) {
        // the registry's MessageTypeIndex refused a duplicate id
        clashes.add(registry.getName() + ": " + e.getCause().getMessage());
        continue;
      }
      Map<Short, Object> byId = new HashMap<>();
      for (Object constant : constants) {
        Object previous = byId.putIfAbsent(((MessageTypeId) constant).id(), constant);
        if (previous != null) {
          clashes.add(registry.getName() + ": " + previous + " and " + constant + " share id " + ((MessageTypeId) constant).id());
        }
      }
    }
    assertEquals(List.of(), clashes);
  }

  /** Finds every enum in the modules' main classes that implements {@link MessageTypeId}. */
  private static List<Class<?>> findRegistries() throws IOException, URISyntaxException {
    Set<Path> locations = new LinkedHashSet<>();
    for (Class<?> anchor : MODULE_ANCHORS) {
      locations.add(Path.of(anchor.getProtectionDomain().getCodeSource().getLocation().toURI()));
    }
    List<Class<?>> registries = new ArrayList<>();
    for (Path location : locations) {
      // a module's classes directory, or its jar once the reactor has packaged it
      if (Files.isDirectory(location)) {
        addRegistries(location, registries);
      } else {
        try (FileSystem jar = FileSystems.newFileSystem(location)) {
          addRegistries(jar.getPath("/"), registries);
        }
      }
    }
    return registries;
  }

  private static void addRegistries(Path root, List<Class<?>> registries) throws IOException {
    ClassLoader loader = MessageTypeRegistriesTest.class.getClassLoader();
    try (Stream<Path> files = Files.walk(root)) {
      for (Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".class"))::iterator) {
        String relative = root.relativize(file).toString();
        String name = relative.substring(0, relative.length() - ".class".length()).replace(file.getFileSystem().getSeparator(), ".");
        Class<?> c;
        try {
          c = Class.forName(name, false, loader);
        } catch (ClassNotFoundException | LinkageError e) {
          continue;
        }
        if (c.isEnum() && MessageTypeId.class.isAssignableFrom(c)) {
          registries.add(c);
        }
      }
    }
  }
}
