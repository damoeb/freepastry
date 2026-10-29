package rice.p2p.util.testing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import rice.p2p.util.XMLObjectInputStream;
import rice.p2p.util.XMLObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * JUnit tests for XMLObjectOutputStream / XMLObjectInputStream.  Runs the checks of
 * XMLObjectStreamUnit, plus regression tests for JDK classes, which are not open to deep
 * reflection on JDK 16+ (no --add-opens needed).
 */
public class XMLObjectStreamTest {

  // ----- the legacy checks of XMLObjectStreamUnit -----

  @Test
  public void legacyPrimitives() throws IOException {
    XMLObjectStreamUnit unit = new XMLObjectStreamUnit();
    unit.testInt(20930);
    unit.testInt(-29384);
    unit.testBoolean(true);
    unit.testBoolean(false);
    unit.testByte((byte) 255);
    unit.testChar('<');
    unit.testChar('"');
    unit.testDouble(-1029.2);
    unit.testFloat((float) 29.239);
    unit.testLong(-19282738339299L);
    unit.testShort((short) 1829);
    unit.testMultiplePrimitives();
  }

  @Test
  public void legacyAll() throws IOException {
    // XMLObjectStreamUnit.start() throws an AssertionError on the first failing check
    new XMLObjectStreamUnit().start();
  }

  // ----- JDK classes -----

  @Test
  public void boxedPrimitives() throws Exception {
    for (Object o : new Object[] { Integer.valueOf(5), Integer.valueOf(100000), Long.valueOf(2837L), Boolean.TRUE,
        Byte.valueOf((byte) -3), Character.valueOf('&'), Double.valueOf(1.5), Float.valueOf(-2.25f), Short.valueOf((short) 7) })
      assertEquals(o, roundTrip(o));
  }

  @Test
  public void jdkCollections() throws Exception {
    Vector<Object> vector = new Vector<Object>(Arrays.asList("a", 1, 2L));
    Hashtable<Object, Object> table = new Hashtable<Object, Object>();
    table.put("monkey", 1020);
    table.put(3, "cat");
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("list", new ArrayList<Object>(Arrays.asList("x", "y")));
    map.put("set", new TreeSet<String>(Arrays.asList("q", "p")));
    map.put("date", new Date(123456789L));
    map.put("big", new BigInteger("123456789012345678901234567890"));

    assertEquals(vector, roundTrip(vector));
    assertEquals(table, roundTrip(table));
    assertEquals(map, roundTrip(map));
    // writeReplace()/readResolve() of the JDK class (serial proxy)
    assertEquals(List.of(1, 2, 3), roundTrip(List.of(1, 2, 3)));
  }

  @Test
  public void enums() throws Exception {
    assertSame(TimeUnit.SECONDS, roundTrip(TimeUnit.SECONDS));
    assertSame(Color.GREEN, roundTrip(Color.GREEN));
  }

  @Test
  public void sharingIsPreservedThroughJdkObjects() throws Exception {
    Node shared = new Node();
    Holder holder = new Holder();
    holder.node = shared;
    holder.list = new ArrayList<Object>(Arrays.asList(shared, shared));
    holder.map = new HashMap<String, Object>();
    holder.map.put("n", shared);
    holder.map.put("l", holder.list);

    Holder copy = (Holder) roundTrip(holder);

    assertNotSame(shared, copy.node);
    assertSame(copy.node, copy.list.get(0));
    assertSame(copy.node, copy.list.get(1));
    assertSame(copy.node, copy.map.get("n"));
    assertSame(copy.list, copy.map.get("l"));
  }

  @Test
  public void selfReferencingJdkCollection() throws Exception {
    ArrayList<Object> list = new ArrayList<Object>();
    list.add("x");
    list.add(list);

    List<?> copy = (List<?>) roundTrip(list);

    assertEquals("x", copy.get(0));
    assertSame(copy, copy.get(1));
  }

  @Test
  public void shortField() throws Exception {
    // regression: short fields were set with Field.setInt(), which throws IllegalArgumentException
    Node node = new Node();
    node.s = 4711;
    assertEquals(4711, ((Node) roundTrip(node)).s);
  }

  // ----- format compatibility for application classes -----

  /** The XML written for Node before the JDK 16+ fix (JDK classes were then serialized via deep reflection). */
  private static final String NODE_XML =
      "<?xml version='1.0'?>\n\n" +
      "<jsx major='1' minor='1' format='JSX.DataReader'>\n" +
      " <object id='i0' class='rice.p2p.util.testing.XMLObjectStreamTest$Node' superclasses=''>\n" +
      "  <declaredClass class='rice.p2p.util.testing.XMLObjectStreamTest$Node'>\n" +
      "   <default>\n" +
      "    <primitive field='s' type='short' value='3' />\n" +
      "    <object id='i1' field='num' class='java.lang.Integer' superclasses='java.lang.Number,'>\n" +
      "     <declaredClass class='java.lang.Number'>\n" +
      "      <default />\n" +
      "     </declaredClass>\n" +
      "     <declaredClass class='java.lang.Integer'>\n" +
      "      <default>\n" +
      "       <primitive field='value' type='int' value='1000' />\n" +
      "      </default>\n" +
      "     </declaredClass>\n" +
      "    </object>\n" +
      "    <object id='i2' field='flag' class='java.lang.Boolean' superclasses=''>\n" +
      "     <declaredClass class='java.lang.Boolean'>\n" +
      "      <default>\n" +
      "       <primitive field='value' type='boolean' value='true' />\n" +
      "      </default>\n" +
      "     </declaredClass>\n" +
      "    </object>\n" +
      "    <string field='name' id='i3' value='node' />\n" +
      "   </default>\n" +
      "  </declaredClass>\n" +
      " </object>\n" +
      "</jsx>\n";

  @Test
  public void applicationClassFormatIsUnchanged() throws Exception {
    Node node = new Node();
    node.s = 3;
    assertEquals(NODE_XML.trim(), new String(write(node), "UTF-8").trim());
  }

  @Test
  public void readsPreviouslyWrittenBoxedValues() throws Exception {
    Node node = (Node) read(NODE_XML.getBytes("UTF-8"));
    assertEquals(3, node.s);
    assertEquals(Integer.valueOf(1000), node.num);
    assertEquals(Boolean.TRUE, node.flag);
    assertEquals("node", node.name);
  }

  // ----- reset() -----

  @Test
  public void resetForgetsReferences() throws Exception {
    Node shared = new Node();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    XMLObjectOutputStream xoos = new XMLObjectOutputStream(baos);
    xoos.writeObject(shared);
    xoos.writeObject(shared);
    xoos.reset();
    xoos.writeObject(shared);
    xoos.reset();
    xoos.writeInt(42);
    xoos.close();

    XMLObjectInputStream xois = new XMLObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    try {
      Object first = xois.readObject();
      assertSame(first, xois.readObject());
      Object afterReset = xois.readObject();
      assertNotSame(first, afterReset, "an object written after reset() is a new copy");
      assertEquals("node", ((Node) afterReset).name);
      assertEquals(42, xois.readInt());
    } finally {
      xois.close();
    }
  }

  // ----- helpers and test classes -----

  private static byte[] write(Object o) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    XMLObjectOutputStream xoos = new XMLObjectOutputStream(baos);
    xoos.writeObject(o);
    xoos.close();
    return baos.toByteArray();
  }

  private static Object read(byte[] data) throws IOException, ClassNotFoundException {
    XMLObjectInputStream xois = new XMLObjectInputStream(new ByteArrayInputStream(data));
    try {
      return xois.readObject();
    } finally {
      xois.close();
    }
  }

  private static Object roundTrip(Object o) throws IOException, ClassNotFoundException {
    return read(write(o));
  }

  enum Color { RED, GREEN }

  static class Node implements Serializable {
    private static final long serialVersionUID = 1L;
    short s;
    Integer num = 1000;
    Boolean flag = Boolean.TRUE;
    String name = "node";
    transient Object ignored = new Object();
  }

  static class Holder implements Serializable {
    private static final long serialVersionUID = 1L;
    Node node;
    ArrayList<Object> list;
    HashMap<String, Object> map;
  }
}
