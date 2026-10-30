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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * Checks the message type-id registries: every enum in the main code that implements
 * {@link MessageTypeId} is one deserializer's scope, so no two of its constants may share an id.
 */
class MessageTypeIdTest {

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

  private enum Clean implements MessageTypeId {
    ONE(1), TWO(2);

    private final short id;

    Clean(int id) {
      this.id = (short) id;
    }

    public short id() {
      return id;
    }
  }

  private enum Clash implements MessageTypeId {
    ONE(1), ALSO_ONE(1);

    private final short id;

    Clash(int id) {
      this.id = (short) id;
    }

    public short id() {
      return id;
    }
  }

  @Test
  void indexFindsConstantsById() {
    MessageTypeIndex<Clean> index = MessageTypeIndex.of(Clean.values());

    assertSame(Clean.TWO, index.get((short) 2));
    assertNull(index.find((short) 3));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> index.get((short) 3));
    assertEquals("Unknown type:3", e.getMessage());
  }

  @Test
  void indexRejectsDuplicateIds() {
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> MessageTypeIndex.of(Clash.values()));
    assertTrue(e.getMessage().contains("ONE") && e.getMessage().contains("ALSO_ONE"), e.getMessage());
  }

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

  /** Finds every enum in the main classes directory that implements {@link MessageTypeId}. */
  private static List<Class<?>> findRegistries() throws IOException, URISyntaxException {
    Path root = Path.of(MessageTypeId.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    ClassLoader loader = MessageTypeIdTest.class.getClassLoader();
    List<Class<?>> registries = new ArrayList<>();
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
    return registries;
  }
}
