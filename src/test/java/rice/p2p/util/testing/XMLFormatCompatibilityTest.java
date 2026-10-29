package rice.p2p.util.testing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import rice.p2p.util.XMLObjectInputStream;
import rice.p2p.util.XMLObjectOutputStream;
import rice.p2p.util.XMLParser;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Regression net for the XML wire and storage format of XMLObjectOutputStream and
 * XMLObjectInputStream. {@link #GOLDEN} was captured with the xpp3 (MXSerializer)
 * based XMLWriter before it was replaced, so the current writer must still produce it
 * byte for byte, and the current reader must still read it.
 */
public class XMLFormatCompatibilityTest {

  @Test
  public void writerProducesGoldenBytes() throws Exception {
    assertEquals(GOLDEN, new String(writeSample(), StandardCharsets.UTF_8));
    assertArrayEquals(GOLDEN.getBytes(StandardCharsets.UTF_8), writeSample());
  }

  @Test
  public void readerReadsGoldenBytes() throws Exception {
    readAndVerifySample(GOLDEN.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void currentOutputRoundTrips() throws Exception {
    readAndVerifySample(writeSample());
  }

  @Test
  public void controlCharactersAreRejectedAsBefore() throws Exception {
    // MXSerializer refused characters below 0x20 other than tab, LF and CR
    XMLObjectOutputStream xoos = new XMLObjectOutputStream(new ByteArrayOutputStream());
    assertThrows(IllegalStateException.class, () -> xoos.writeObject("bell\u0007"));
  }

  @Test
  public void charactersIllegalInXml10RoundTrip() throws Exception {
    // the writer emits U+FFFE/U+FFFF unescaped (as MXSerializer did); a StAX parser would reject them
    String s = "max￿￾char";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    XMLObjectOutputStream xoos = new XMLObjectOutputStream(baos);
    xoos.writeObject(s);
    xoos.close();
    XMLObjectInputStream xois = new XMLObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(s, xois.readObject());
    xois.close();
  }

  @Test
  public void parserReportsEventTypeAndDecodesEntities() throws Exception {
    XMLParser parser = new XMLParser();
    parser.setInput(new StringReader("<?xml version='1.0'?><a v='&amp;lt;&#10;&foo;&apos;'>x&lt;y</a>"));
    assertEquals(XMLParser.START_DOCUMENT, parser.getEventType());
    assertEquals(XMLParser.START_TAG, parser.next());
    assertEquals(XMLParser.START_TAG, parser.getEventType());
    assertEquals("&lt;\n&foo;'", parser.getAttributeValue(null, "v"));
    assertEquals(XMLParser.TEXT, parser.next());
    assertEquals("x<y", parser.getText());
    assertEquals(XMLParser.END_TAG, parser.next());
    assertEquals("a", parser.getName());
  }

  @Test
  public void doctypeAndEntityDeclarationsAreRejected() throws Exception {
    String xxe = "<?xml version='1.0'?>\n<!DOCTYPE jsx [<!ENTITY x SYSTEM 'file:///etc/passwd'>]>\n"
        + "<jsx major='1' minor='1' format='JSX.DataReader'>\n <string value='&x;' />\n</jsx>\n";
    assertThrows(IOException.class, () -> {
      XMLObjectInputStream xois = new XMLObjectInputStream(new ByteArrayInputStream(xxe.getBytes(StandardCharsets.UTF_8)));
      xois.readObject();
    });
  }

  // ----- the sample -----

  /** Writes a representative sequence of objects and primitives. */
  static byte[] writeSample() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    XMLObjectOutputStream xoos = new XMLObjectOutputStream(baos);
    xoos.writeInt(-17);
    xoos.writeBoolean(false);
    xoos.writeChar('&');
    xoos.writeDouble(-0.0);
    xoos.writeUTF("utf <&'\"> ä");
    xoos.write(new byte[] { 1, 2, 3, -1, 0, 127 });
    xoos.writeObject(Everything.create());
    xoos.writeObject("second");
    xoos.writeObject(null);
    xoos.writeLong(Long.MAX_VALUE);
    xoos.close();
    return baos.toByteArray();
  }

  static void readAndVerifySample(byte[] data) throws Exception {
    XMLObjectInputStream xois = new XMLObjectInputStream(new ByteArrayInputStream(data));
    assertEquals(-17, xois.readInt());
    assertFalse(xois.readBoolean());
    assertEquals('&', xois.readChar());
    assertEquals(Double.doubleToLongBits(-0.0), Double.doubleToLongBits(xois.readDouble()));
    assertEquals("utf <&'\"> ä", xois.readUTF());
    byte[] bytes = new byte[6];
    xois.readFully(bytes);
    assertArrayEquals(new byte[] { 1, 2, 3, -1, 0, 127 }, bytes);
    Everything.create().verify((Everything) xois.readObject());
    assertEquals("second", xois.readObject());
    assertNull(xois.readObject());
    assertEquals(Long.MAX_VALUE, xois.readLong());
    xois.close();
  }

  enum Color { RED, GREEN }

  static class Child implements Serializable {
    private static final long serialVersionUID = 1L;
    String name;
    Child next;

    Child(String name) {
      this.name = name;
    }
  }

  /** Not serializable superclass with a no-arg constructor, as Java serialization requires. */
  static class Base {
    transient int baseValue = 99;
  }

  /** Uses custom writeObject/readObject with extra stream data. */
  static class Custom extends Base implements Serializable {
    private static final long serialVersionUID = 1L;
    int regular = 5;
    transient String extra = "extra & more";

    private void writeObject(ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
      out.writeInt(123);
      out.writeObject(extra);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      assertEquals(123, in.readInt());
      extra = (String) in.readObject();
    }
  }

  /** Uses putFields()/readFields(). */
  static class Fields implements Serializable {
    private static final long serialVersionUID = 1L;
    int a = 1;
    String b = "two";

    private void writeObject(ObjectOutputStream out) throws IOException {
      ObjectOutputStream.PutField fields = out.putFields();
      fields.put("a", a);
      fields.put("b", b);
      out.writeFields();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      ObjectInputStream.GetField fields = in.readFields();
      a = fields.get("a", 0);
      b = (String) fields.get("b", null);
    }
  }

  static class Ext implements Externalizable {
    private static final long serialVersionUID = 1L;
    int value;

    public Ext() {
    }

    Ext(int value) {
      this.value = value;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(value);
      out.writeObject("ext'value");
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      value = in.readInt();
      assertEquals("ext'value", in.readObject());
    }
  }

  static class Everything implements Serializable {
    private static final long serialVersionUID = 1L;
    int i;
    long l;
    short s;
    byte b;
    char c;
    boolean z;
    float f;
    double d;
    Integer boxedInt;
    Long boxedLong;
    Character boxedChar;
    Double boxedNaN;
    Float boxedFloat;
    Short boxedShort;
    Byte boxedByte;
    Boolean boxedBoolean;
    String plain;
    String special;
    String unicode;
    String empty;
    String blank;
    String nullString;
    Object nullObject;
    int[] ints;
    byte[] bytes;
    char[] chars;
    boolean[] bools;
    long[] longs;
    double[][] matrix;
    String[] strings;
    Object[] mixed;
    Child child;
    Child sameChild;
    Everything self;
    ArrayList<Object> list;
    TreeMap<String, Object> map;
    Date date;
    BigInteger big;
    TimeUnit unit;
    Color color;
    Custom custom;
    Fields fields;
    Ext ext;
    Child deep;
    transient String skipped = "transient";

    static Everything create() {
      Everything e = new Everything();
      e.i = -42;
      e.l = Long.MIN_VALUE;
      e.s = 12345;
      e.b = -128;
      e.c = '\'';
      e.z = true;
      e.f = 1.25e-3f;
      e.d = Math.PI;
      e.boxedInt = 7;
      e.boxedLong = 1L << 40;
      e.boxedChar = '<';
      e.boxedNaN = Double.NaN;
      e.boxedFloat = Float.NEGATIVE_INFINITY;
      e.boxedShort = (short) -1;
      e.boxedByte = (byte) 64;
      e.boxedBoolean = Boolean.FALSE;
      e.plain = "hello";
      e.special = "<tag attr=\"v\" other='w'> & &amp; ]]> \t\n\r\n end";
      e.unicode = "äöü € 😀   中文";
      e.empty = "";
      e.blank = "   ";
      e.ints = new int[] { 1, -2, Integer.MAX_VALUE };
      e.bytes = new byte[] { 0, 1, -1, 127, -128 };
      e.chars = new char[] { 'a', '&', '"', '\t' };
      e.bools = new boolean[] { true, false };
      e.longs = new long[0];
      e.matrix = new double[][] { { 1.0, 2.5 }, { -3.0 }, null };
      e.strings = new String[] { "a", null, "a", "<b>" };
      e.child = new Child("shared");
      e.sameChild = e.child;
      e.mixed = new Object[] { "x", 3, e.child, null, new int[] { 9 } };
      e.self = e;
      e.list = new ArrayList<Object>(Arrays.asList("one", 2, e.child, e.child));
      e.map = new TreeMap<String, Object>();
      e.map.put("k&1", "v<1>");
      e.map.put("k2", e.ints);
      e.date = new Date(1234567890123L);
      e.big = new BigInteger("-123456789012345678901234567890");
      e.unit = TimeUnit.MILLISECONDS;
      e.color = Color.GREEN;
      e.custom = new Custom();
      e.fields = new Fields();
      e.ext = new Ext(77);
      // deep enough to exceed MXSerializer's maximum indentation (65 levels)
      Child head = new Child("d0");
      Child current = head;
      for (int n = 1; n < 30; n++) {
        current.next = new Child("d" + n);
        current = current.next;
      }
      e.deep = head;
      return e;
    }

    void verify(Everything r) {
      assertEquals(i, r.i);
      assertEquals(l, r.l);
      assertEquals(s, r.s);
      assertEquals(b, r.b);
      assertEquals(c, r.c);
      assertEquals(z, r.z);
      assertEquals(f, r.f);
      assertEquals(d, r.d);
      assertEquals(boxedInt, r.boxedInt);
      assertEquals(boxedLong, r.boxedLong);
      assertEquals(boxedChar, r.boxedChar);
      assertEquals(boxedNaN, r.boxedNaN);
      assertEquals(boxedFloat, r.boxedFloat);
      assertEquals(boxedShort, r.boxedShort);
      assertEquals(boxedByte, r.boxedByte);
      assertEquals(boxedBoolean, r.boxedBoolean);
      assertEquals(plain, r.plain);
      assertEquals(special, r.special);
      assertEquals(unicode, r.unicode);
      assertEquals(empty, r.empty);
      assertEquals(blank, r.blank);
      assertNull(r.nullString);
      assertNull(r.nullObject);
      assertArrayEquals(ints, r.ints);
      assertArrayEquals(bytes, r.bytes);
      assertArrayEquals(chars, r.chars);
      assertArrayEquals(bools, r.bools);
      assertArrayEquals(longs, r.longs);
      assertArrayEquals(matrix[0], r.matrix[0]);
      assertArrayEquals(matrix[1], r.matrix[1]);
      assertNull(r.matrix[2]);
      assertArrayEquals(strings, r.strings);
      assertSame(r.strings[0], r.strings[2]);
      assertEquals("shared", r.child.name);
      assertSame(r.child, r.sameChild);
      assertEquals("x", r.mixed[0]);
      assertEquals(3, r.mixed[1]);
      assertSame(r.child, r.mixed[2]);
      assertNull(r.mixed[3]);
      assertArrayEquals(new int[] { 9 }, (int[]) r.mixed[4]);
      assertSame(r, r.self);
      assertEquals(Arrays.asList("one", 2), r.list.subList(0, 2));
      assertSame(r.child, r.list.get(2));
      assertSame(r.child, r.list.get(3));
      assertEquals("v<1>", r.map.get("k&1"));
      assertSame(r.ints, r.map.get("k2"));
      assertEquals(date, r.date);
      assertEquals(big, r.big);
      assertSame(unit, r.unit);
      assertSame(color, r.color);
      assertEquals(5, r.custom.regular);
      assertEquals("extra & more", r.custom.extra);
      assertEquals(99, r.custom.baseValue);
      assertEquals(1, r.fields.a);
      assertEquals("two", r.fields.b);
      assertEquals(77, r.ext.value);
      Child current = r.deep;
      for (int n = 0; n < 30; n++) {
        assertEquals("d" + n, current.name);
        current = current.next;
      }
      assertNull(current);
      assertNull(r.skipped);
    }
  }

  // ----- golden output, captured with the xpp3 based writer -----

  static final String GOLDEN =
      "<?xml version='1.0'?>\n"
    + "\n"
    + "<jsx major='1' minor='1' format='JSX.DataReader'>\n"
    + " <primitive type='int' value='-17' />\n"
    + " <primitive type='boolean' value='false' />\n"
    + " <primitive type='char' value='&amp;' />\n"
    + " <primitive type='double' value='-0.0' />\n"
    + " <string id='i0' value='utf &lt;&amp;&apos;\"> \u00e4' />\n"
    + " <base64 length='6'>AQID/wB/\n"
    + " </base64>\n"
    + " <object id='i1' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Everything' superclasses=''>\n"
    + "  <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Everything'>\n"
    + "   <default>\n"
    + "    <primitive field='i' type='int' value='-42' />\n"
    + "    <primitive field='l' type='long' value='-9223372036854775808' />\n"
    + "    <primitive field='s' type='short' value='12345' />\n"
    + "    <primitive field='b' type='byte' value='-128' />\n"
    + "    <primitive field='c' type='char' value='&apos;' />\n"
    + "    <primitive field='z' type='boolean' value='true' />\n"
    + "    <primitive field='f' type='float' value='0.00125' />\n"
    + "    <primitive field='d' type='double' value='3.141592653589793' />\n"
    + "    <object id='i2' field='boxedInt' class='java.lang.Integer' superclasses='java.lang.Number,'>\n"
    + "     <declaredClass class='java.lang.Number'>\n"
    + "      <default />\n"
    + "     </declaredClass>\n"
    + "     <declaredClass class='java.lang.Integer'>\n"
    + "      <default>\n"
    + "       <primitive field='value' type='int' value='7' />\n"
    + "      </default>\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "    <object id='i3' field='boxedLong' class='java.lang.Long' superclasses='java.lang.Number,'>\n"
    + "     <declaredClass class='java.lang.Number'>\n"
    + "      <default />\n"
    + "     </declaredClass>\n"
    + "     <declaredClass class='java.lang.Long'>\n"
    + "      <default>\n"
    + "       <primitive field='value' type='long' value='1099511627776' />\n"
    + "      </default>\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "    <object id='i4' field='boxedChar' class='java.lang.Character' superclasses=''>\n"
    + "     <declaredClass class='java.lang.Character'>\n"
    + "      <default>\n"
    + "       <primitive field='value' type='char' value='&lt;' />\n"
    + "      </default>\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "    <object id='i5' field='boxedNaN' class='java.lang.Double' superclasses='java.lang.Number,'>\n"
    + "     <declaredClass class='java.lang.Number'>\n"
    + "      <default />\n"
    + "     </declaredClass>\n"
    + "     <declaredClass class='java.lang.Double'>\n"
    + "      <default>\n"
    + "       <primitive field='value' type='double' value='NaN' />\n"
    + "      </default>\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "    <object id='i6' field='boxedFloat' class='java.lang.Float' superclasses='java.lang.Number,'>\n"
    + "     <declaredClass class='java.lang.Number'>\n"
    + "      <default />\n"
    + "     </declaredClass>\n"
    + "     <declaredClass class='java.lang.Float'>\n"
    + "      <default>\n"
    + "       <primitive field='value' type='float' value='-Infinity' />\n"
    + "      </default>\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "    <object id='i7' field='boxedShort' class='java.lang.Short' superclasses='java.lang.Number,'>\n"
    + "     <declaredClass class='java.lang.Number'>\n"
    + "      <default />\n"
    + "     </declaredClass>\n"
    + "     <declaredClass class='java.lang.Short'>\n"
    + "      <default>\n"
    + "       <primitive field='value' type='short' value='-1' />\n"
    + "      </default>\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "    <object id='i8' field='boxedByte' class='java.lang.Byte' superclasses='java.lang.Number,'>\n"
    + "     <declaredClass class='java.lang.Number'>\n"
    + "      <default />\n"
    + "     </declaredClass>\n"
    + "     <declaredClass class='java.lang.Byte'>\n"
    + "      <default>\n"
    + "       <primitive field='value' type='byte' value='64' />\n"
    + "      </default>\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "    <object id='i9' field='boxedBoolean' class='java.lang.Boolean' superclasses=''>\n"
    + "     <declaredClass class='java.lang.Boolean'>\n"
    + "      <default>\n"
    + "       <primitive field='value' type='boolean' value='false' />\n"
    + "      </default>\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "    <string field='plain' id='i10' value='hello' />\n"
    + "    <string field='special' id='i11' value='&lt;tag attr=\"v\" other=&apos;w&apos;> &amp; &amp;amp; ]]> &#9;&#10;&#13;&#10; end' />\n"
    + "    <string field='unicode' id='i12' value='\u00e4\u00f6\u00fc \u20ac \ud83d\ude00 \u00a0 \u4e2d\u6587' />\n"
    + "    <string field='empty' id='i13' value='' />\n"
    + "    <string field='blank' id='i14' value='   ' />\n"
    + "    <null field='nullString' />\n"
    + "    <null field='nullObject' />\n"
    + "    <array field='ints' id='i15' base='int' dim='1' length='3'>\n"
    + "     <primitive type='int' value='1' />\n"
    + "     <primitive type='int' value='-2' />\n"
    + "     <primitive type='int' value='2147483647' />\n"
    + "    </array>\n"
    + "    <array field='bytes' id='i16' base='byte' dim='1' length='5'>\n"
    + "     <primitive type='byte' value='0' />\n"
    + "     <primitive type='byte' value='1' />\n"
    + "     <primitive type='byte' value='-1' />\n"
    + "     <primitive type='byte' value='127' />\n"
    + "     <primitive type='byte' value='-128' />\n"
    + "    </array>\n"
    + "    <array field='chars' id='i17' base='char' dim='1' length='4'>\n"
    + "     <primitive type='char' value='a' />\n"
    + "     <primitive type='char' value='&amp;' />\n"
    + "     <primitive type='char' value='\"' />\n"
    + "     <primitive type='char' value='&#9;' />\n"
    + "    </array>\n"
    + "    <array field='bools' id='i18' base='boolean' dim='1' length='2'>\n"
    + "     <primitive type='boolean' value='true' />\n"
    + "     <primitive type='boolean' value='false' />\n"
    + "    </array>\n"
    + "    <array field='longs' id='i19' base='long' dim='1' length='0' />\n"
    + "    <array field='matrix' id='i20' base='[D' dim='2' length='3'>\n"
    + "     <array id='i21' base='double' dim='1' length='2'>\n"
    + "      <primitive type='double' value='1.0' />\n"
    + "      <primitive type='double' value='2.5' />\n"
    + "     </array>\n"
    + "     <array id='i22' base='double' dim='1' length='1'>\n"
    + "      <primitive type='double' value='-3.0' />\n"
    + "     </array>\n"
    + "     <null />\n"
    + "    </array>\n"
    + "    <array field='strings' id='i23' base='java.lang.String' dim='1' length='4'>\n"
    + "     <string id='i24' value='a' />\n"
    + "     <null />\n"
    + "     <reference idref='i24' />\n"
    + "     <string id='i25' value='&lt;b>' />\n"
    + "    </array>\n"
    + "    <array field='mixed' id='i26' base='java.lang.Object' dim='1' length='5'>\n"
    + "     <string id='i27' value='x' />\n"
    + "     <object id='i28' class='java.lang.Integer' superclasses='java.lang.Number,'>\n"
    + "      <declaredClass class='java.lang.Number'>\n"
    + "       <default />\n"
    + "      </declaredClass>\n"
    + "      <declaredClass class='java.lang.Integer'>\n"
    + "       <default>\n"
    + "        <primitive field='value' type='int' value='3' />\n"
    + "       </default>\n"
    + "      </declaredClass>\n"
    + "     </object>\n"
    + "     <object id='i29' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "      <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "       <default>\n"
    + "        <string field='name' id='i30' value='shared' />\n"
    + "        <null field='next' />\n"
    + "       </default>\n"
    + "      </declaredClass>\n"
    + "     </object>\n"
    + "     <null />\n"
    + "     <array id='i31' base='int' dim='1' length='1'>\n"
    + "      <primitive type='int' value='9' />\n"
    + "     </array>\n"
    + "    </array>\n"
    + "    <reference field='child' idref='i29' />\n"
    + "    <reference field='sameChild' idref='i29' />\n"
    + "    <reference field='self' idref='i1' />\n"
    + "    <jserial id='i32' field='list' class='java.util.ArrayList'>\n"
    + "     <base64 length='167'>rO0ABXNyABNqYXZhLnV0aWwuQXJyYXlMaXN0eIHSHZnHYZ0DAAFJAARzaXpleHAAAAAEdwQAAAAE\n"
    + "c3IAN3JpY2UucDJwLnV0aWwuWE1MT2JqZWN0T3V0cHV0U3RyZWFtJEphdmFTZXJpYWxpemVkQ2hp\n"
    + "bGQAAAAAAAAAAQIAAUkABWluZGV4eHAAAAAAc3EAfgACAAAAAXNxAH4AAgAAAAJxAH4ABXg=\n"
    + "     </base64>\n"
    + "     <string id='i33' value='one' />\n"
    + "     <object id='i34' class='java.lang.Integer' superclasses='java.lang.Number,'>\n"
    + "      <declaredClass class='java.lang.Number'>\n"
    + "       <default />\n"
    + "      </declaredClass>\n"
    + "      <declaredClass class='java.lang.Integer'>\n"
    + "       <default>\n"
    + "        <primitive field='value' type='int' value='2' />\n"
    + "       </default>\n"
    + "      </declaredClass>\n"
    + "     </object>\n"
    + "     <reference idref='i29' />\n"
    + "    </jserial>\n"
    + "    <jserial id='i35' field='map' class='java.util.TreeMap'>\n"
    + "     <base64 length='198'>rO0ABXNyABFqYXZhLnV0aWwuVHJlZU1hcAzB9j4tJWrmAwABTAAKY29tcGFyYXRvcnQAFkxqYXZh\n"
    + "L3V0aWwvQ29tcGFyYXRvcjt4cHB3BAAAAAJzcgA3cmljZS5wMnAudXRpbC5YTUxPYmplY3RPdXRw\n"
    + "dXRTdHJlYW0kSmF2YVNlcmlhbGl6ZWRDaGlsZAAAAAAAAAABAgABSQAFaW5kZXh4cAAAAABzcQB+\n"
    + "AAMAAAABc3EAfgADAAAAAnNxAH4AAwAAAAN4\n"
    + "     </base64>\n"
    + "     <string id='i36' value='k&amp;1' />\n"
    + "     <string id='i37' value='v&lt;1>' />\n"
    + "     <string id='i38' value='k2' />\n"
    + "     <reference idref='i15' />\n"
    + "    </jserial>\n"
    + "    <jserial id='i39' field='date' class='java.util.Date'>\n"
    + "     <base64 length='46'>rO0ABXNyAA5qYXZhLnV0aWwuRGF0ZWhqgQFLWXQZAwAAeHB3CAAAAR9x+wTLeA==\n"
    + "     </base64>\n"
    + "    </jserial>\n"
    + "    <jserial id='i40' field='big' class='java.math.BigInteger'>\n"
    + "     <base64 length='263'>rO0ABXNyABRqYXZhLm1hdGguQmlnSW50ZWdlcoz8nx+pO/sdAwAGSQAIYml0Q291bnRJAAliaXRM\n"
    + "ZW5ndGhJABNmaXJzdE5vbnplcm9CeXRlTnVtSQAMbG93ZXN0U2V0Qml0SQAGc2lnbnVtWwAJbWFn\n"
    + "bml0dWRldAACW0J4cgAQamF2YS5sYW5nLk51bWJlcoaslR0LlOCLAgAAeHD///////////////7/\n"
    + "///+/////3NyADdyaWNlLnAycC51dGlsLlhNTE9iamVjdE91dHB1dFN0cmVhbSRKYXZhU2VyaWFs\n"
    + "aXplZENoaWxkAAAAAAAAAAECAAFJAAVpbmRleHhwAAAAAHg=\n"
    + "     </base64>\n"
    + "     <array id='i41' base='byte' dim='1' length='13'>\n"
    + "      <primitive type='byte' value='1' />\n"
    + "      <primitive type='byte' value='-114' />\n"
    + "      <primitive type='byte' value='-23' />\n"
    + "      <primitive type='byte' value='15' />\n"
    + "      <primitive type='byte' value='-10' />\n"
    + "      <primitive type='byte' value='-61' />\n"
    + "      <primitive type='byte' value='115' />\n"
    + "      <primitive type='byte' value='-32' />\n"
    + "      <primitive type='byte' value='-18' />\n"
    + "      <primitive type='byte' value='78' />\n"
    + "      <primitive type='byte' value='63' />\n"
    + "      <primitive type='byte' value='10' />\n"
    + "      <primitive type='byte' value='-46' />\n"
    + "     </array>\n"
    + "    </jserial>\n"
    + "    <jserial id='i42' field='unit' class='java.util.concurrent.TimeUnit'>\n"
    + "     <base64 length='94'>rO0ABX5yAB1qYXZhLnV0aWwuY29uY3VycmVudC5UaW1lVW5pdAAAAAAAAAAAEgAAeHIADmphdmEu\n"
    + "bGFuZy5FbnVtAAAAAAAAAAASAAB4cHQADE1JTExJU0VDT05EUw==\n"
    + "     </base64>\n"
    + "    </jserial>\n"
    + "    <jserial id='i43' field='color' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Color'>\n"
    + "     <base64 length='112'>rO0ABX5yADZyaWNlLnAycC51dGlsLnRlc3RpbmcuWE1MRm9ybWF0Q29tcGF0aWJpbGl0eVRlc3Qk\n"
    + "Q29sb3IAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAVHUkVFTg==\n"
    + "     </base64>\n"
    + "    </jserial>\n"
    + "    <object id='i44' field='custom' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Custom' superclasses='rice.p2p.util.testing.XMLFormatCompatibilityTest$Base,'>\n"
    + "     <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Custom'>\n"
    + "      <default>\n"
    + "       <primitive field='regular' type='int' value='5' />\n"
    + "      </default>\n"
    + "      <primitive type='int' value='123' />\n"
    + "      <string id='i45' value='extra &amp; more' />\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "    <object id='i46' field='fields' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Fields' superclasses=''>\n"
    + "     <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Fields'>\n"
    + "      <default>\n"
    + "       <primitive field='a' type='int' value='1' />\n"
    + "       <string field='b' id='i47' value='two' />\n"
    + "      </default>\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "    <object id='i48' field='ext' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Ext' superclasses=''>\n"
    + "     <primitive type='int' value='77' />\n"
    + "     <string id='i49' value='ext&apos;value' />\n"
    + "    </object>\n"
    + "    <object id='i50' field='deep' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "     <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "      <default>\n"
    + "       <string field='name' id='i51' value='d0' />\n"
    + "       <object id='i52' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "        <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "         <default>\n"
    + "          <string field='name' id='i53' value='d1' />\n"
    + "          <object id='i54' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "           <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "            <default>\n"
    + "             <string field='name' id='i55' value='d2' />\n"
    + "             <object id='i56' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "              <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "               <default>\n"
    + "                <string field='name' id='i57' value='d3' />\n"
    + "                <object id='i58' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                 <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                  <default>\n"
    + "                   <string field='name' id='i59' value='d4' />\n"
    + "                   <object id='i60' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                    <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                     <default>\n"
    + "                      <string field='name' id='i61' value='d5' />\n"
    + "                      <object id='i62' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                       <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                        <default>\n"
    + "                         <string field='name' id='i63' value='d6' />\n"
    + "                         <object id='i64' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                          <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                           <default>\n"
    + "                            <string field='name' id='i65' value='d7' />\n"
    + "                            <object id='i66' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                             <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                              <default>\n"
    + "                               <string field='name' id='i67' value='d8' />\n"
    + "                               <object id='i68' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                 <default>\n"
    + "                                  <string field='name' id='i69' value='d9' />\n"
    + "                                  <object id='i70' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                   <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                    <default>\n"
    + "                                     <string field='name' id='i71' value='d10' />\n"
    + "                                     <object id='i72' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                      <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                       <default>\n"
    + "                                        <string field='name' id='i73' value='d11' />\n"
    + "                                        <object id='i74' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                         <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                          <default>\n"
    + "                                           <string field='name' id='i75' value='d12' />\n"
    + "                                           <object id='i76' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                            <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                             <default>\n"
    + "                                              <string field='name' id='i77' value='d13' />\n"
    + "                                              <object id='i78' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                               <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                <default>\n"
    + "                                                 <string field='name' id='i79' value='d14' />\n"
    + "                                                 <object id='i80' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                  <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                   <default>\n"
    + "                                                    <string field='name' id='i81' value='d15' />\n"
    + "                                                    <object id='i82' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                     <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                      <default>\n"
    + "                                                       <string field='name' id='i83' value='d16' />\n"
    + "                                                       <object id='i84' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                        <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                         <default>\n"
    + "                                                          <string field='name' id='i85' value='d17' />\n"
    + "                                                          <object id='i86' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                           <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                            <default>\n"
    + "                                                             <string field='name' id='i87' value='d18' />\n"
    + "                                                             <object id='i88' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                              <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                               <default>\n"
    + "                                                                <string field='name' id='i89' value='d19' />\n"
    + "                                                                <object id='i90' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                                <default>\n"
    + "                                                                <string field='name' id='i91' value='d20' />\n"
    + "                                                                <object id='i92' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                                <default>\n"
    + "                                                                <string field='name' id='i93' value='d21' />\n"
    + "                                                                <object id='i94' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                                <default>\n"
    + "                                                                <string field='name' id='i95' value='d22' />\n"
    + "                                                                <object id='i96' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                                <default>\n"
    + "                                                                <string field='name' id='i97' value='d23' />\n"
    + "                                                                <object id='i98' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                                <default>\n"
    + "                                                                <string field='name' id='i99' value='d24' />\n"
    + "                                                                <object id='i100' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                                <default>\n"
    + "                                                                <string field='name' id='i101' value='d25' />\n"
    + "                                                                <object id='i102' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                                <default>\n"
    + "                                                                <string field='name' id='i103' value='d26' />\n"
    + "                                                                <object id='i104' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                                <default>\n"
    + "                                                                <string field='name' id='i105' value='d27' />\n"
    + "                                                                <object id='i106' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                                <default>\n"
    + "                                                                <string field='name' id='i107' value='d28' />\n"
    + "                                                                <object id='i108' field='next' class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child' superclasses=''>\n"
    + "                                                                <declaredClass class='rice.p2p.util.testing.XMLFormatCompatibilityTest$Child'>\n"
    + "                                                                <default>\n"
    + "                                                                <string field='name' id='i109' value='d29' />\n"
    + "                                                                <null field='next' />\n"
    + "                                                                </default>\n"
    + "                                                                </declaredClass>\n"
    + "                                                                </object>\n"
    + "                                                                </default>\n"
    + "                                                                </declaredClass>\n"
    + "                                                                </object>\n"
    + "                                                                </default>\n"
    + "                                                                </declaredClass>\n"
    + "                                                                </object>\n"
    + "                                                                </default>\n"
    + "                                                                </declaredClass>\n"
    + "                                                                </object>\n"
    + "                                                                </default>\n"
    + "                                                                </declaredClass>\n"
    + "                                                                </object>\n"
    + "                                                                </default>\n"
    + "                                                                </declaredClass>\n"
    + "                                                                </object>\n"
    + "                                                                </default>\n"
    + "                                                                </declaredClass>\n"
    + "                                                                </object>\n"
    + "                                                                </default>\n"
    + "                                                                </declaredClass>\n"
    + "                                                                </object>\n"
    + "                                                                </default>\n"
    + "                                                                </declaredClass>\n"
    + "                                                                </object>\n"
    + "                                                                </default>\n"
    + "                                                                </declaredClass>\n"
    + "                                                                </object>\n"
    + "                                                               </default>\n"
    + "                                                              </declaredClass>\n"
    + "                                                             </object>\n"
    + "                                                            </default>\n"
    + "                                                           </declaredClass>\n"
    + "                                                          </object>\n"
    + "                                                         </default>\n"
    + "                                                        </declaredClass>\n"
    + "                                                       </object>\n"
    + "                                                      </default>\n"
    + "                                                     </declaredClass>\n"
    + "                                                    </object>\n"
    + "                                                   </default>\n"
    + "                                                  </declaredClass>\n"
    + "                                                 </object>\n"
    + "                                                </default>\n"
    + "                                               </declaredClass>\n"
    + "                                              </object>\n"
    + "                                             </default>\n"
    + "                                            </declaredClass>\n"
    + "                                           </object>\n"
    + "                                          </default>\n"
    + "                                         </declaredClass>\n"
    + "                                        </object>\n"
    + "                                       </default>\n"
    + "                                      </declaredClass>\n"
    + "                                     </object>\n"
    + "                                    </default>\n"
    + "                                   </declaredClass>\n"
    + "                                  </object>\n"
    + "                                 </default>\n"
    + "                                </declaredClass>\n"
    + "                               </object>\n"
    + "                              </default>\n"
    + "                             </declaredClass>\n"
    + "                            </object>\n"
    + "                           </default>\n"
    + "                          </declaredClass>\n"
    + "                         </object>\n"
    + "                        </default>\n"
    + "                       </declaredClass>\n"
    + "                      </object>\n"
    + "                     </default>\n"
    + "                    </declaredClass>\n"
    + "                   </object>\n"
    + "                  </default>\n"
    + "                 </declaredClass>\n"
    + "                </object>\n"
    + "               </default>\n"
    + "              </declaredClass>\n"
    + "             </object>\n"
    + "            </default>\n"
    + "           </declaredClass>\n"
    + "          </object>\n"
    + "         </default>\n"
    + "        </declaredClass>\n"
    + "       </object>\n"
    + "      </default>\n"
    + "     </declaredClass>\n"
    + "    </object>\n"
    + "   </default>\n"
    + "  </declaredClass>\n"
    + " </object>\n"
    + " <string id='i110' value='second' />\n"
    + " <null />\n"
    + " <primitive type='long' value='9223372036854775807' />\n"
    + "</jsx>\n";
}
