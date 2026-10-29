package rice.p2p.util.testing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import rice.environment.params.simple.SimpleParameters;
import rice.p2p.commonapi.Message;
import rice.p2p.util.Base64;
import rice.p2p.util.JavaSerializationFilter;
import rice.p2p.util.JavaSerializationFilter.RejectedClassException;
import rice.p2p.util.rawserialization.JavaDeserializer;
import rice.p2p.util.rawserialization.JavaSerializedDeserializer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.pastry.Id;
import rice.pastry.messaging.PastryObjectInputStream;
import rice.pastry.standard.InitiatePingNeighbor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The deserialization filter admits what FreePastry serializes, rejects everything else with an
 * actionable message, and can be extended per environment (parameter) or JVM-wide (API).
 */
public class JavaSerializationFilterTest {

  /** a Serializable JDK class that is not on the default allowlist */
  static final Object NOT_ALLOWED = new java.awt.Point(1, 2);

  @AfterEach
  void cleanUp() {
    JavaSerializationFilter.disallowGlobally("java.awt.*");
  }

  static byte[] serialize(Object o) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bytes)) {
      oos.writeObject(o);
    }
    return bytes.toByteArray();
  }

  static Object readWithJavaDeserializer(byte[] data) throws Exception {
    try (ObjectInputStream ois = new JavaDeserializer(new ByteArrayInputStream(data), null)) {
      return ois.readObject();
    }
  }

  static SimpleParameters params(String allow) {
    SimpleParameters p = new SimpleParameters(new String[0], null);
    if (allow != null) p.setString(JavaSerializationFilter.PARAM_ALLOW, allow);
    return p;
  }

  static RejectedClassException rejection(InvalidClassException ice) {
    assertInstanceOf(RejectedClassException.class, ice.getCause(), "cause of " + ice);
    return (RejectedClassException) ice.getCause();
  }

  @Test
  void typicalFreePastryPayloadRoundTrips() throws Exception {
    HashMap<String, Object> payload = new HashMap<String, Object>();
    payload.put("id", Id.build("0123456789ABCDEF0123456789ABCDEF01234567"));
    payload.put("ids", new Id[] {Id.build(), Id.build("FF")});
    payload.put("list", new ArrayList<Object>(List.of(1, 2L, 3.0, "four", 'f', (byte) 6, (short) 7, true)));
    payload.put("immutable", List.of("a", "b"));
    payload.put("unmodifiable", Collections.unmodifiableMap(new TreeMap<String, Integer>(Map.of("x", 1))));
    payload.put("sets", new Object[] {new HashSet<Integer>(Set.of(1)), new TreeSet<String>(Set.of("s")), new LinkedHashSet<String>()});
    payload.put("concurrent", new ConcurrentHashMap<String, String>(Map.of("k", "v")));
    payload.put("legacy", new Object[] {new Vector<Integer>(List.of(1)), new Hashtable<String, String>(Map.of("h", "t")), new LinkedList<String>()});
    payload.put("addr", new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 9001));
    payload.put("addr6", InetAddress.getByName("::1"));
    payload.put("values", new Object[] {new Date(1234), new BigInteger("12345678901234567890"), UUID.randomUUID(), TimeUnit.SECONDS, new Random(42)});
    payload.put("primitives", new Object[] {new byte[] {1, 2}, new int[][] {{1}, {2}}, new long[0]});
    payload.put("exception", new IOException("io", new IllegalStateException("cause")));

    Object read = readWithJavaDeserializer(serialize(payload));

    assertEquals(payload.keySet(), ((Map<?, ?>) read).keySet());
    assertEquals(payload.get("id"), ((Map<?, ?>) read).get("id"));
    assertEquals(payload.get("list"), ((Map<?, ?>) read).get("list"));
    assertEquals("io", ((IOException) ((Map<?, ?>) read).get("exception")).getMessage());
  }

  @Test
  void javaSerializedPastryMessageRoundTrips() throws Exception {
    InitiatePingNeighbor msg = new InitiatePingNeighbor();

    try (ObjectInputStream ois = new PastryObjectInputStream(new ByteArrayInputStream(serialize(msg)), null)) {
      assertInstanceOf(InitiatePingNeighbor.class, ois.readObject());
    }
  }

  @Test
  void commonApiDeserializerRejectsDisallowedClassFromNetwork() throws Exception {
    JavaSerializedDeserializer deserializer = new JavaSerializedDeserializer(null);

    InvalidClassException ice = assertThrows(InvalidClassException.class,
        () -> deserializer.deserialize(new SimpleInputBuffer(serialize(NOT_ALLOWED)), (short) 0, 0, null));

    assertTrue(rejection(ice).getMessage().contains("java.awt.Point"), rejection(ice).getMessage());
  }

  @Test
  void disallowedClassNestedInAllowedContainerIsRejectedWithHowToAllow() {
    ArrayList<Object> list = new ArrayList<Object>(List.of("ok", NOT_ALLOWED));

    InvalidClassException ice = assertThrows(InvalidClassException.class, () -> readWithJavaDeserializer(serialize(list)));

    String message = rejection(ice).getMessage();
    assertTrue(message.contains("java.awt.Point"), message);
    assertTrue(message.contains(JavaSerializationFilter.PARAM_ALLOW), message);
    assertTrue(message.contains("allowGlobally"), message);
  }

  @Test
  void pastryObjectInputStreamRejectsDisallowedClass() {
    assertThrows(InvalidClassException.class, () -> {
      try (ObjectInputStream ois = new PastryObjectInputStream(new ByteArrayInputStream(serialize(NOT_ALLOWED)), null)) {
        ois.readObject();
      }
    });
  }

  @Test
  void gadgetClassIsDeniedEvenWhenAllowedByPattern() throws Exception {
    JavaSerializationFilter permissive = new JavaSerializationFilter(List.of("javax.**", "java.net.*"), 0, 0, 0, 0);

    assertFalse(permissive.isAllowed(javax.management.BadAttributeValueExpException.class));
    assertFalse(permissive.isAllowed(java.net.URL.class));
    InvalidClassException ice = assertThrows(InvalidClassException.class,
        () -> readWithJavaDeserializer(serialize(new javax.management.BadAttributeValueExpException("x"))));
    assertTrue(rejection(ice).getMessage().contains("deny list"), rejection(ice).getMessage());
  }

  @Test
  void base64DecodeToObjectIsFiltered() throws Exception {
    assertEquals("hello", Base64.decodeToObject(Base64.encodeBytes(serialize("hello"))));
    assertNull(Base64.decodeToObject(Base64.encodeBytes(serialize(NOT_ALLOWED))));
  }

  @Test
  void parameterExtendsAllowlist() throws Exception {
    JavaSerializationFilter filter = JavaSerializationFilter.forParameters(params("com.example.**; java.awt.Point"));
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serialize(NOT_ALLOWED)));
    ois.setObjectInputFilter(filter);

    assertEquals(NOT_ALLOWED, ois.readObject());
    assertFalse(JavaSerializationFilter.forParameters(params(null)).isAllowed(java.awt.Point.class));
  }

  @Test
  void apiExtendsAllowlistGlobally() throws Exception {
    JavaSerializationFilter.allowGlobally("java.awt.*");

    assertEquals(NOT_ALLOWED, readWithJavaDeserializer(serialize(NOT_ALLOWED)));
  }

  @Test
  void filterCanBeDisabledByParameter() {
    SimpleParameters p = params(null);
    p.setBoolean(JavaSerializationFilter.PARAM_ENABLED, false);

    assertNull(JavaSerializationFilter.forParameters(p));
  }

  @Test
  void depthLimitIsEnforced() throws Exception {
    List<Object> deep = new ArrayList<Object>();
    List<Object> current = deep;
    for (int i = 0; i < 300; i++) {
      List<Object> next = new ArrayList<Object>();
      current.add(next);
      current = next;
    }

    InvalidClassException ice = assertThrows(InvalidClassException.class, () -> readWithJavaDeserializer(serialize(deep)));
    assertTrue(rejection(ice).getMessage().contains(JavaSerializationFilter.PARAM_MAX_DEPTH), rejection(ice).getMessage());
  }

  @Test
  void arrayLimitIsEnforced() throws Exception {
    SimpleParameters p = params(null);
    p.setString(JavaSerializationFilter.PARAM_MAX_ARRAY, "10");
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serialize(new byte[11])));
    ois.setObjectInputFilter(JavaSerializationFilter.forParameters(p));

    assertThrows(InvalidClassException.class, ois::readObject);
  }

  @Test
  void invalidPatternIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> JavaSerializationFilter.allowGlobally("!java.awt.*"));
    assertThrows(IllegalArgumentException.class, () -> JavaSerializationFilter.allowGlobally("maxdepth=5"));
  }

  @Test
  void deserializedMessageKeepsType() throws Exception {
    Message m = (Message) readWithJavaDeserializer(serialize(new rice.tutorial.lesson3.MyMsg(Id.build("01"), Id.build("02"))));
    assertNotNull(m);
  }
}
