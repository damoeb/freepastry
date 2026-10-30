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

package rice.p2p.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;

/**
 * XMLWriter is a utility class used by XMLObjectOutputStream to perform the actual
 * XML writing.
 *
 * The output format is the one the xpp3 MXSerializer produced (configured with a one
 * space indentation, "\n" line separators and apostrophes around attribute values),
 * which this class reproduces byte for byte so that the XML written stays identical
 * after the xmlpull/xpp3 libraries were dropped.  The JDK's StAX XMLStreamWriter
 * cannot be used for that: it always quotes attributes with '"' and escapes
 * differently.  The rules, taken over from MXSerializer, are:
 * <ul>
 * <li>a start tag is preceded by a newline and (depth - 1) spaces, except for the
 * root element and right after text; indentation is capped at 64 spaces</li>
 * <li>an end tag is indented the same way, unless the element has no content, in
 * which case it is written as " /&gt;"</li>
 * <li>in attribute values, '&amp;', '&lt;' and '\'' are escaped as entities and tab,
 * LF and CR as character references; other characters below 0x20 are rejected with
 * an IllegalStateException</li>
 * </ul>
 *
 * @version $Id: XMLWriter.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class XMLWriter {

  /**
   * The maximum indentation, in characters, as in MXSerializer
   */
  private static final int MAX_INDENT = 65;

  /**
   * A newline followed by MAX_INDENT spaces, prefixes of which are written as indentation
   */
  private static final char[] INDENTATION = ("\n" + " ".repeat(MAX_INDENT)).toCharArray();

  /**
   * The underlying writer which the XML is written to
   */
  private final Writer writer;

  /**
   * The names of the currently open elements
   */
  private final ArrayList<String> open;

  /**
   * Whether the last start tag still lacks its closing '&gt;' (attributes can be added)
   */
  private boolean startTagIncomplete;

  /**
   * Whether the last thing written was a tag (and not text), so the next tag is indented
   */
  private boolean seenTag;

  /**
   * Constructor which takes the provided writer and
   * builds a new XML writier to read XML from the writier.
   *
   * @param out The writer to base this XML writer off of
   * @throws IOException If an error occurs
   */
  public XMLWriter(OutputStream out) throws IOException {
    this.writer = new BufferedWriter(new OutputStreamWriter(out));
    this.open = new ArrayList<String>();
  }

  /**
   * Method which flushes all buffered data to the underlying writer
   *
   * @throws IOException If an error occurs
   */
  public void flush() throws IOException {
    closeStartTag();
    writer.flush();
  }

  /**
   * Method which flushes and closes the underlying writer, which will
   * cause future writer attempts to throw an IOException.
   *
   * @throws IOException If an error occurs
   */
  public void close() throws IOException {
    text("\n");
    flush();
    writer.close();
  }

  /**
   * Method which writes a sequence of base64 encoded bytes to the output stream
   *
   * @param bytes The bytes to write
   */
  public void writeBase64(byte[] bytes, int off, int len) throws IOException {
    flush();
    writer.write(Base64.encodeBytes(bytes, off, len));
  }

  /**
   * Method which writes the XML header to the writer.
   *
   * @throws IOException If an error occurs
   */
  public void writeHeader() throws IOException {
    writer.write("<?xml version='1.0'?>");
    text("\n\n");
  }

  /**
   * Method which writes an attribute to the XML document.
   *
   * @param name The name of the attribute to write
   * @param value The value to write
   * @throws IOException If an error occurs
   */
  public void attribute(String name, int value) throws IOException {
    attribute(name, String.valueOf(value));
  }

  /**
   * Method which writes an attribute to the XML document.
   *
   * @param name The name of the attribute to write
   * @param value The value to write
   * @throws IOException If an error occurs
   */
  public void attribute(String name, double value) throws IOException {
    attribute(name, String.valueOf(value));
  }

  /**
   * Method which writes an attribute to the XML document.
   *
   * @param name The name of the attribute to write
   * @param value The value to write
   * @throws IOException If an error occurs
   */
  public void attribute(String name, float value) throws IOException {
    attribute(name, String.valueOf(value));
  }

  /**
   * Method which writes an attribute to the XML document.
   *
   * @param name The name of the attribute to write
   * @param value The value to write
   * @throws IOException If an error occurs
   */
  public void attribute(String name, long value) throws IOException {
    attribute(name, String.valueOf(value));
  }

  /**
   * Method which writes an attribute to the XML document.
   *
   * @param name The name of the attribute to write
   * @param value The value to write
   * @throws IOException If an error occurs
   */
  public void attribute(String name, char value) throws IOException {
    attribute(name, String.valueOf(value));
  }

  /**
   * Method which writes an attribute to the XML document.
   *
   * @param name The name of the attribute to write
   * @param value The value to write
   * @throws IOException If an error occurs
   */
  public void attribute(String name, boolean value) throws IOException {
    attribute(name, String.valueOf(value));
  }

  /**
   * Method which writes an attribute to the XML document.
   *
   * @param name The name of the attribute to write
   * @param value The value to write
   * @throws IOException If an error occurs
   */
  public void attribute(String name, Object value) throws IOException {
    if (value == null)
      return;

    attribute(name, value.toString());
  }

  /**
   * Method which writes an attribute to the XML document.
   *
   * @param name The name of the attribute to write
   * @param value The value to write
   * @throws IOException If an error occurs
   */
  protected void attribute(String name, String value) throws IOException {
    if (! startTagIncomplete)
      throw new IllegalArgumentException("start() must be called before attribute()");

    writer.write(' ');
    writer.write(name);
    writer.write("='");
    writeAttributeValue(value);
    writer.write('\'');
  }

  /**
   * Method which starts the given tag name
   *
   * @param name The name of the tag to start
   * @throws IOException If an error occurs
   */
  public void start(String name) throws IOException {
    closeStartTag();
    open.add(name);

    if (seenTag)
      writeIndent();

    seenTag = true;
    startTagIncomplete = true;
    writer.write('<');
    writer.write(name);
  }

  /**
   * Method which ends the given tag name
   *
   * @param name The name of the tag to end
   * @throws IOException If an error occurs
   */
  public void end(String name) throws IOException {
    if (open.isEmpty() || ! open.get(open.size() - 1).equals(name))
      throw new IllegalArgumentException("expected element name " + (open.isEmpty() ? null : open.get(open.size() - 1)) + " and not " + name);

    if (startTagIncomplete) {
      writer.write(" />");
      startTagIncomplete = false;
    } else {
      if (seenTag)
        writeIndent();

      writer.write("</");
      writer.write(name);
      writer.write('>');
    }

    open.remove(open.size() - 1);
    seenTag = true;
  }

  /**
   * Internal method which writes element content.  Only used for the whitespace between
   * the header, the root element and the end of the document, so it is not escaped.
   *
   * @param text The text to write
   * @throws IOException If an error occurs
   */
  protected void text(String text) throws IOException {
    closeStartTag();
    seenTag = false;
    writer.write(text);
  }

  /**
   * Internal method which finishes the current start tag, if any, with a '&gt;'
   *
   * @throws IOException If an error occurs
   */
  protected void closeStartTag() throws IOException {
    if (startTagIncomplete) {
      writer.write('>');
      startTagIncomplete = false;
    }
  }

  /**
   * Internal method which writes the indentation for a tag at the current depth
   *
   * @throws IOException If an error occurs
   */
  protected void writeIndent() throws IOException {
    writer.write(INDENTATION, 0, Math.min(open.size(), MAX_INDENT));
  }

  /**
   * Internal method which writes the escaped value of an attribute
   *
   * @param value The value to write
   * @throws IOException If an error occurs
   */
  protected void writeAttributeValue(String value) throws IOException {
    int length = value.length();
    int last = 0;

    for (int i=0; i<length; i++) {
      char c = value.charAt(i);
      String escaped;

      switch (c) {
        case '&': escaped = "&amp;"; break;
        case '<': escaped = "&lt;"; break;
        case '\'': escaped = "&apos;"; break;
        case '\t': escaped = "&#9;"; break;
        case '\n': escaped = "&#10;"; break;
        case '\r': escaped = "&#13;"; break;
        default:
          if (c < ' ')
            throw new IllegalStateException("character " + (int) c + " is not allowed in output (attr value=" + value + ")");
          continue;
      }

      writer.write(value, last, i - last);
      writer.write(escaped);
      last = i + 1;
    }

    writer.write(value, last, length - last);
  }
}
