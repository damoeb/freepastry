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

import java.io.*;
import java.util.*;

/** 
* This class is a memory-efficient implementation of most of the XML
* pull parsing API (http://www.xmlpull.org).  It used to implement the xmlpull
* XmlPullParser interface; it is now self-contained so FreePastry no longer depends
* on the xmlpull/xpp3 libraries.  The event constants keep their xmlpull values.
*
* Only the subset of XML that XMLWriter produces is supported.  In particular, DTDs,
* entity declarations and external entities are never processed (a DOCTYPE is
* rejected as malformed), and only the predefined entities and the character
* references &amp;#9; &amp;#10; and &amp;#13; are decoded, so the parser is not
* susceptible to XXE or entity expansion attacks.
*
* A hand-written parser is kept instead of the JDK's StAX parser because it is
* faster on this format and, unlike StAX, accepts every character that XMLWriter
* writes (e.g. U+FFFE/U+FFFF in strings, which are not legal XML 1.0 characters).
*/
public class XMLParser {

  /** Event types, with the values of the xmlpull XmlPullParser constants */
  public static final int START_DOCUMENT = 0;
  public static final int END_DOCUMENT = 1;
  public static final int START_TAG = 2;
  public static final int END_TAG = 3;
  public static final int TEXT = 4;
  public static final int CDSECT = 5;
  public static final int ENTITY_REF = 6;
  public static final int IGNORABLE_WHITESPACE = 7;
  public static final int PROCESSING_INSTRUCTION = 8;
  public static final int COMMENT = 9;
  public static final int DOCDECL = 10;

  /**
   * The size of the internal buffer to allocate
   */
  public static final int BUFFER_SIZE = 32000;
  public static final int MAX_ATTRIBUTES = 100;

  public static final char[] QUOTE = new char[] {'\'', '"'};
  public static final char[] TAG_END = new char[] {'>', '/', '?'};
  public static final char[] WHITESPACE = new char[] {' ', '\t', '\n', '\r'};
  public static final char[] WHITESPACE_OR_TAG_END = new char[] {' ', '\t', '\n', '\r', '>', '/', '?'};
  public static final char[] WHITESPACE_OR_EQUALS = new char[] {' ', '\t', '\n', '\r', '='};
  public static final char[] SINGLE = new char[1];
  
  public static final String[][] ENTITIES = new String[][] { {"&apos;", "'"}, 
                                                             {"&quot;", "\""}, 
                                                             {"&lt;", "<"},
                                                             {"&gt;", ">"},
                                                             {"&#13;", String.valueOf((char) 13)},
                                                             {"&#10;", String.valueOf((char) 10)},
                                                             {"&#9;", String.valueOf((char) 9)},
                                                             {"&amp;", "&"} };

  /**
   * The internal reader used to read data
   */
  protected Reader reader;
  
  /**
   * The internal buffer used to process data
   */
  protected char[] buffer;
  
  /**
   * Internal pointers into the buffer
   */
  protected int bufferPosition;
  protected int bufferLimit;
  
  /**
   * The StringCache used to reduce the memory requirements
   */
  protected StringCache cache;
  
  /**
   * The internal stack of tags which have been read
   */
  protected Stack<String> tags;
  
  /**
   * If the tag parsed was a start/end, the name of the tag
   */
  protected String name;
  
  /**
   * If the tag parsed was text, the text
   */
  protected String text;
  
  /**
   * If the tag parsed was a start tag, the list of attribute-> value pairs
   */
  protected String[] attributeKeys = new String[MAX_ATTRIBUTES];
  protected String[] attributeValues = new String[MAX_ATTRIBUTES];
  protected int numAttributes;
  
  /**
   * Whether or not we are currently in a tag...
   */
  protected boolean inTag;

  /**
   * The type of the last event returned by next()
   */
  protected int eventType = START_DOCUMENT;
  
  /**
   * Internal variable which keeps track of the current mark
   */
  protected int mark;
  protected CharArrayBuffer marked;
  
  /**
   * Constructor
   */
  public XMLParser() {
    this.buffer = new char[BUFFER_SIZE];
    this.bufferPosition = 0;
    this.bufferLimit = 0;
    this.mark = -1;
    this.tags = new Stack<String>();
    this.cache = new StringCache();
    this.inTag = false;
    this.numAttributes = 0;
  }
  
  /**
   * Set the input source for parser to the given reader and
   * resets the parser. The event type is set to the initial value
   * START_DOCUMENT.
   * Setting the reader to null will just stop parsing and
   * reset parser state,
   * allowing the parser to free internal resources
   * such as parsing buffers.
   */
  public void setInput(Reader in) throws IOException {
    this.reader = in;
  }
  
  /**
   * Returns the text content of the current event as String.
   * The value returned depends on current event type,
   * for example for TEXT event it is element content
   * (this is typical case when next() is used).
   *
   * See description of nextToken() for detailed description of
   * possible returned values for different types of events.
   *
   * <p><strong>NOTE:</strong> in case of ENTITY_REF, this method returns
   * the entity replacement text (or null if not available). This is
   * the only case where
   * getText() and getTextCharacters() return different values.
   *
   * @see #getEventType
   * @see #next
   * @see #nextToken
   */
  public String getText() {
    return this.text;
  }
  
  /**
   * For START_TAG or END_TAG events, the (local) name of the current
   * element is returned when namespaces are enabled. When namespace
   * processing is disabled, the raw name is returned.
   * For ENTITY_REF events, the entity name is returned.
   * If the current event is not START_TAG, END_TAG, or ENTITY_REF,
   * null is returned.
   * <p><b>Please note:</b> To reconstruct the raw element name
   *  when namespaces are enabled and the prefix is not null,
   * you will need to  add the prefix and a colon to localName..
   *
   */
  public String getName() {
    return this.name;
  }
  
  /**
   * Returns the attributes value identified by namespace URI and namespace localName.
   * If namespaces are disabled namespace must be null.
   * If current event type is not START_TAG then IndexOutOfBoundsException will be thrown.
   *
   * <p><strong>NOTE:</strong> attribute value must be normalized
   * (including entity replacement text if PROCESS_DOCDECL is false) as described in
   * <a href="http://www.w3.org/TR/REC-xml#AVNormalize">XML 1.0 section
   * 3.3.3 Attribute-Value Normalization</a>
   *
   * @see #defineEntityReplacementText
   *
   * @param namespace Namespace of the attribute if namespaces are enabled otherwise must be null
   * @param name If namespaces enabled local name of attribute otherwise just attribute name
   * @return value of attribute or null if attribute with given name does not exist
   */
  public String getAttributeValue(String namespace, String name) {
    for (int i=0; i<numAttributes; i++)
      if (attributeKeys[i].equals(name))
        return attributeValues[i];
    
    return null;
  }
  
  /**
   * Returns the type of the current event (START_TAG, END_TAG, TEXT, etc.)
   *
   * @see #next()
   * @see #nextToken()
   */
  public int getEventType() throws IOException {
    return eventType;
  }
  
  /**
   * Get next parsing event - element content wil be coalesced and only one
   * TEXT event must be returned for whole element content
   * (comments and processing instructions will be ignored and emtity references
   * must be expanded or exception mus be thrown if entity reerence can not be exapnded).
   * If element content is empty (content is "") then no TEXT event will be reported.
   *
   * <p><b>NOTE:</b> empty element (such as &lt;tag/>) will be reported
   *  with  two separate events: START_TAG, END_TAG - it must be so to preserve
   *   parsing equivalency of empty element to &lt;tag>&lt;/tag>.
   *  (see isEmptyElementTag ())
   *
   * @see #isEmptyElementTag
   * @see #START_TAG
   * @see #TEXT
   * @see #END_TAG
   * @see #END_DOCUMENT
   */
  public int next() throws IOException {
    return (eventType = nextEvent());
  }

  /**
   * Internal method which parses the next event
   */
  protected int nextEvent() throws IOException {
    char next = 0;
    
    try {
      next = current();
    } catch (EOFException e) {
      return END_DOCUMENT;
    }

    switch (next) {
      case '<':
        int result = parseTag();
        
        if (result == START_DOCUMENT)
          return nextEvent();
        else
          return result;
      case '/':
        if (this.inTag)
          return parseEndTag((String) tags.pop());
        else
          return parseText();
      default:
        return parseText();
    }
  }
  
  /**
   * Checks whether the current TEXT event contains only whitespace
   * characters.
   * For IGNORABLE_WHITESPACE, this is always true.
   * For TEXT and CDSECT, false is returned when the current event text
   * contains at least one non-white space character. For any other
   * event type an exception is thrown.
   *
   * <p><b>Please note:</b> non-validating parsers are not
   * able to distinguish whitespace and ignorable whitespace,
   * except from whitespace outside the root element. Ignorable
   * whitespace is reported as separate event, which is exposed
   * via nextToken only.
   *
   */
  public boolean isWhitespace() throws IOException {
    return isWhitespace(text);
  }
  
  /**
   *   ----- INTERNAL PARSING METHODS -----
   */
  
  /**
   * Internal method which actually fills the buffer
   */
  protected void fillBuffer() throws IOException {
    // process the mark
    if (marked != null)
      this.marked.append(buffer, 0, bufferPosition);
    else if (mark != -1)
      this.marked = new CharArrayBuffer(buffer, mark, bufferPosition - mark);
    
    int read = reader.read(buffer);
    
    if (read > 0) {
      bufferLimit = read;
      bufferPosition = 0;
    }
  }
  
  /**
   * Method which returns the current char in the buffer
   *
   * @return The current char
   */
  protected char current() throws IOException {
    if (bufferPosition == bufferLimit) fillBuffer();
    if (bufferPosition == bufferLimit) throw new EOFException();
    
    return buffer[bufferPosition];
  }
  
  /**
   * Method which steps forward in the buffer
   */
  protected void step() {
    bufferPosition++;
  }
  
  /**
   * Sets the mark
   */
  protected void mark() {
    this.mark = bufferPosition;
  }
  
  /**
   * Unsets the mark
   */
  protected String unmark() {
    try {    
      if (this.marked != null) {
        this.marked.append(buffer, 0, bufferPosition);
        return cache.get(marked.getBuffer(), 0, marked.getLength());
      } else {
        return cache.get(buffer, mark, bufferPosition-mark);
      }
    } finally {
      this.mark = -1;
      this.marked = null;
    }
  }
  
  /**
   * Internal method which clears the list of attributes
   */
  protected void clearAttributes() {
    this.numAttributes = 0;
  }
  
  /**
   * Internal method which adds an attributes
   */
  protected void addAttribute(String key, String value) {
    if (numAttributes == attributeKeys.length)
      throw new RuntimeException("More than "+attributeKeys.length+" attributes encountered - unsupported!");
    
    attributeKeys[numAttributes] = key;
    attributeValues[numAttributes] = value;
    numAttributes++;
  }
  
  /**
   * An assertion method
   *
   * @param the expected char
   */
  protected void expect(char c) throws IOException {
    if (current() != c) 
      throw new IOException("Expected character '" + c + "' got '" + current() + "'");
    else
      step();
  }
  
  /**
   * Internal method which checks for existence
   *
   * @param chars The chars to check for
   * @param char The char
   */
  public boolean isWhitespace(String text) {
    for (int i=0; i<text.length(); i++)
      if (! contains(WHITESPACE, text.charAt(i)))
        return false;
    
    return true;
  }
  
  /**
   * Internal method which checks for existence
   *
   * @param chars The chars to check for
   * @param char The char
   */
  protected boolean contains(char[] chars, char c) {
    for (int i=0; i<chars.length; i++)
      if (chars[i] == c)
        return true;
    
    return false;
  }
  
  /**
   * Method which parses and returns up to the next token
   *
   * @return The token
   */
  protected String parseUntil(char[] chars) throws IOException {
    mark();
    
    while (true) {
      if (contains(chars, current()))
        break;
      else
        step();
    }
    
    return unmark();
  }
  
  /**
   * Method which parses and returns up to the next token
   *
   * @return The token
   */
  protected String parseUntil(char c) throws IOException {
    mark();
    
    while (true) {
      if (current() == c)
        break;
      else
        step();
    }
    
    return unmark();
  }
  
  /**
   * Method which parses up to the next token
   */
  protected void parseUntilNot(char[] chars) throws IOException {
    while (true) {
      if (! contains(chars, current()))
        break;
      else
        step();
    }    
  }
    
  /**
   * Method which parses an end tag of the form <tag />
   *
   * @param The name of the parsed tag
   */
  protected int parseEndTag(String tag) throws IOException {
    expect('/');
    expect('>');

    this.name = tag;
    this.inTag = false;
    
//    System.out.println("Parsed default END tag '" + this.name + "'");
    
    return END_TAG;
  }
  
  /**
   * Internal method which parses a tag
   */
  protected int parseTag() throws IOException {
    expect('<');

    switch (current()) {
      case '/':
        return parseEndTag();
      case '?':
        return parseDocumentTag();
      default:
        return parseStartTag();
    }    
  }
  
  /**
   * Method which parses an end tag of the form <tag />
   *
   * @param The name of the parsed tag
   */
  protected int parseEndTag() throws IOException {
    expect('/');
    parseUntilNot(WHITESPACE);
 
    clearAttributes();
    this.name = parseUntil(WHITESPACE_OR_TAG_END);
    tags.pop();
    this.inTag = false;
    
    parseUntilNot(WHITESPACE);
    expect('>');

//    System.out.println("Parsed END tag '" + this.name + "'");
    
    return END_TAG;
  }
  
  /**
   * Method which parses a start tag
   */
  protected int parseStartTag() throws IOException {
    parseUntilNot(WHITESPACE);
    
    this.name = parseUntil(WHITESPACE_OR_TAG_END);
    tags.push(this.name);
    
    parseUntilNot(WHITESPACE);

//    System.out.println("Parsed START tag '" + this.name + "'");
    
    parseAttributes();
    
    parseUntilNot(WHITESPACE);
        
    if (current() != '/') {
      expect('>');
      this.inTag = false;
    } else {
      this.inTag = true;
    }
    
    return START_TAG;
  }
  
  /**
   * Method which parses a document tag
   */
  protected int parseDocumentTag() throws IOException {
    expect('?');
    
    parseUntilNot(WHITESPACE);
    
    String type = parseUntil(WHITESPACE_OR_TAG_END);

    if (! (type.toLowerCase().equals("xml")))
      throw new IOException("This does not appear to be an XML document - found '" + type + "'!");
    
    parseUntilNot(WHITESPACE);
    
    parseAttributes();
    clearAttributes();
    
    parseUntilNot(WHITESPACE);
    this.inTag = false;
    
    expect('?');
    expect('>');
    
//    System.out.println("Parsed START DOCUMENT tag '" + type + "'");
    
    return START_DOCUMENT;
  }
  
  /**
   * Method which parses all of the attributes of a start tag
   */
  protected void parseAttributes() throws IOException {
    clearAttributes();

    while (true) {
      parseUntilNot(WHITESPACE);
      
      if (contains(TAG_END, current())) {
        return;
      } else {
        String key = parseUntil(WHITESPACE_OR_EQUALS);
        parseUntilNot(WHITESPACE);
        expect('=');
        parseUntilNot(WHITESPACE);
        String value = null;
        char quote = current();
        
        if (contains(QUOTE, quote)) {
          expect(quote);
          value = convert(parseUntil(quote));
          expect(quote);
        } else {
          value = convert(parseUntil(WHITESPACE_OR_TAG_END));
        }
        
//        System.out.println("Parsed ATTRIBUTE '" + key + "' -> '"  + value + "'");
        
        addAttribute(key, value);
      }
    }
  }
  
  /**
   * Method which parses an end tag of the form <tag />
   *
   * @param The name of the parsed tag
   */
  protected int parseText() throws IOException {
    clearAttributes();
    this.text = convert(parseUntil('<'));
    this.inTag = false;

    return TEXT;
  }
  
  /**
   * Internal method which deconverts all of the HTML/XML entities
   * like &amp, &gt, &lt, etc...
   *
   * @param string The string to convert
   * @return The result
   */
  protected String convert(String string) {
    int amp = string.indexOf('&');
    
    if (amp < 0)
      return string;
    
    // Single pass over the string.  Equivalent to replacing each ENTITIES pattern in
    // turn (as this method used to do with replaceAll()): &amp; was decoded last there,
    // so a decoded '&' is never decoded again, and unknown entities are kept verbatim.
    StringBuilder result = new StringBuilder(string.length());
    int last = 0;
    
    while (amp >= 0) {
      String[] entity = matchEntity(string, amp);
      
      if (entity != null) {
        result.append(string, last, amp).append(entity[1]);
        last = amp + entity[0].length();
        amp = string.indexOf('&', last);
      } else {
        amp = string.indexOf('&', amp + 1);
      }
    }
    
    return result.append(string, last, string.length()).toString(); 
  }
  
  /**
   * Internal method which returns the entry of ENTITIES starting at the given
   * offset of the string, or null if there is none
   */
  private static String[] matchEntity(String string, int offset) {
    for (int i=0; i<ENTITIES.length; i++)
      if (string.startsWith(ENTITIES[i][0], offset))
        return ENTITIES[i];
    
    return null;
  }
  
  /**
   *   ----- INTERNAL CLASS -----
   */
  
  /**
    * This class implements a char array buffer
   */
  public class CharArrayBuffer {
    
    /**
    * The default initial capacity
     */
    public static final int DEFAULT_CAPACITY = 32;
    
    /**
    * The internal buffer
     */
    protected char[] buffer;
    
    /**
      * The markers
     */
    protected int length;
    
    /**
      * Constructor
     */
    public CharArrayBuffer(char[] chars, int length, int off) {
      this.buffer = new char[DEFAULT_CAPACITY];
      this.length = 0;
      
      append(chars, length, off);
    }
    
    /**
      * Returns the internal array
     *
     * @return The array
     */
    public char[] getBuffer() {
      return buffer;
    }
    
    /**
      * Returns the length
     *
     * @return The length
     */
    public int getLength() {
      return length;
    }
    
    /**
      * Appends some more chars!
     *
     * @param THe chars to append
     */
    public void append(char[] chars, int off, int len) {
      while (length + len > buffer.length)
        expandBuffer();
      
      System.arraycopy(chars, off, buffer, length, len);
      length+=len;
    }
    
    /**
      * Expands the buffer
     */
    protected void expandBuffer() {
      char[] newbuffer = new char[buffer.length * 2];
      
      System.arraycopy(buffer, 0, newbuffer, 0, length);
      this.buffer = newbuffer;
    }
  }
    
  /**
   *   ----- UNSUPPORTED METHODS -----
   */
  
  public void setFeature(String name, boolean state) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public boolean getFeature(String name) {
    throw new UnsupportedOperationException();
  }
  
  public void setProperty(String name, Object value) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public Object getProperty(String name) {
    throw new UnsupportedOperationException();
  }
  
  public void setInput(InputStream inputStream, String inputEncoding) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public String getInputEncoding() {
    throw new UnsupportedOperationException();
  }
  
  public void defineEntityReplacementText(String entityName, String replacementText ) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public int getNamespaceCount(int depth) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public String getNamespacePrefix(int pos) throws IOException  {
    throw new UnsupportedOperationException();
  }
  
  public String getNamespaceUri(int pos) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public String getNamespace (String prefix) {
    throw new UnsupportedOperationException();
  }
  
  public int getDepth() {
    throw new UnsupportedOperationException();
  }
    
  public String getPositionDescription () {
    throw new UnsupportedOperationException();
  }
  
  public int getLineNumber() {
    throw new UnsupportedOperationException();
  }
  
  public int getColumnNumber() {
    throw new UnsupportedOperationException();
  }
    
  public char[] getTextCharacters(int [] holderForStartAndLength) {
    throw new UnsupportedOperationException();
  }
  
  public String getNamespace () {
    throw new UnsupportedOperationException();
  }
  
  public String getPrefix() {
    throw new UnsupportedOperationException();
  }
  
  public boolean isEmptyElementTag() throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public String getAttributeNamespace (int index) {
    throw new UnsupportedOperationException();
  }
  
  public String getAttributePrefix(int index) {
    throw new UnsupportedOperationException();
  }
  
  public String getAttributeType(int index) {
    throw new UnsupportedOperationException();
  }
  
  public boolean isAttributeDefault(int index) {
    throw new UnsupportedOperationException();
  }
  
  public int nextToken() throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public void require(int type, String namespace, String name) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public String nextText() throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public int nextTag() throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public int getAttributeCount() {
    throw new UnsupportedOperationException();
  }
  
  public String getAttributeName(int index) {
    throw new UnsupportedOperationException();
  } 
  
  public String getAttributeValue(int index) {
    throw new UnsupportedOperationException();
  }
}

