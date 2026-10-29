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

import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import rice.environment.Environment;
import rice.environment.params.Parameters;

/**
 * The {@link ObjectInputFilter} that FreePastry installs on every {@link ObjectInputStream} that reads
 * Java-serialized data (network messages, PAST/Scribe content, persistent metadata).
 * <p>
 * The filter is an <b>allowlist</b>: a class is accepted only if it matches {@link #DEFAULT_ALLOW}, an
 * application pattern (parameter {@value #PARAM_ALLOW} or {@link #allowGlobally(String...)}), or the
 * JVM-wide filter ({@code -Djdk.serialFilter}) explicitly allows it. Known gadget-prone classes
 * ({@link #DEFAULT_DENY}) are always rejected, even when an allow pattern matches them. Graph limits
 * (depth, references, bytes, array length) bound resource usage.
 * <p>
 * Patterns use the {@code jdk.serialFilter} syntax: {@code a.b.C} (exact class), {@code a.b.*} (classes
 * in package {@code a.b}), {@code a.b.**} (package and subpackages), {@code a.b.C*} (any class name with
 * that prefix). Array types are checked by their component type; primitive arrays are always allowed.
 * <p>
 * A rejected class makes {@code readObject()} throw an {@link java.io.InvalidClassException} whose
 * cause is a {@link RejectedClassException} explaining how to allow the class.
 *
 * @author FreePastry
 */
public final class JavaSerializationFilter implements ObjectInputFilter {

  /** Set to false to disable the filter (NOT recommended: deserializing untrusted data is code execution). */
  public static final String PARAM_ENABLED = "java_serialization_filter_enabled";
  /** Additional allowed patterns, separated by ';', ',' or whitespace. Example: {@code com.example.myapp.**} */
  public static final String PARAM_ALLOW = "java_serialization_filter_allow";
  public static final String PARAM_MAX_DEPTH = "java_serialization_filter_maxdepth";
  public static final String PARAM_MAX_REFS = "java_serialization_filter_maxrefs";
  public static final String PARAM_MAX_BYTES = "java_serialization_filter_maxbytes";
  public static final String PARAM_MAX_ARRAY = "java_serialization_filter_maxarray";

  public static final long DEFAULT_MAX_DEPTH = 200;
  public static final long DEFAULT_MAX_REFS = 1_000_000;
  public static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;
  public static final long DEFAULT_MAX_ARRAY = 16L * 1024 * 1024;

  /** What FreePastry itself serializes: its own classes and the JDK value/collection types they use. */
  public static final List<String> DEFAULT_ALLOW = List.of(
      "rice.**",
      "org.mpisws.**",
      // java.lang values (Number/Enum/Object appear as superclass or array component descriptors)
      "java.lang.String", "java.lang.Boolean", "java.lang.Character", "java.lang.Byte", "java.lang.Short",
      "java.lang.Integer", "java.lang.Long", "java.lang.Float", "java.lang.Double", "java.lang.Number",
      "java.lang.Enum", "java.lang.Object", "java.lang.StringBuffer", "java.lang.StringBuilder",
      "java.lang.StackTraceElement", "java.lang.String$CaseInsensitiveComparator",
      "java.math.BigInteger", "java.math.BigDecimal",
      // collections
      "java.util.ArrayList", "java.util.LinkedList", "java.util.Vector", "java.util.Stack",
      "java.util.HashMap", "java.util.LinkedHashMap", "java.util.TreeMap", "java.util.Hashtable",
      "java.util.Properties", "java.util.IdentityHashMap", "java.util.EnumMap", "java.util.EnumSet*",
      "java.util.HashSet", "java.util.LinkedHashSet", "java.util.TreeSet", "java.util.ArrayDeque",
      "java.util.PriorityQueue", "java.util.BitSet", "java.util.Collections$*", "java.util.Arrays$ArrayList",
      "java.util.CollSer", "java.util.ImmutableCollections$*", "java.util.AbstractMap$Simple*", "java.util.Map$Entry",
      "java.util.concurrent.ConcurrentHashMap*", "java.util.concurrent.ConcurrentLinkedQueue",
      "java.util.concurrent.ConcurrentSkipList*", "java.util.concurrent.CopyOnWriteArray*",
      "java.util.concurrent.LinkedBlockingQueue", "java.util.concurrent.atomic.*",
      // ConcurrentHashMap writes its legacy Segment (a ReentrantLock) for compatibility
      "java.util.concurrent.locks.ReentrantLock*", "java.util.concurrent.locks.AbstractQueuedSynchronizer",
      "java.util.concurrent.locks.AbstractOwnableSynchronizer",
      // other values
      "java.util.Date", "java.util.Random", "java.util.UUID", "java.util.Locale", "java.time.Ser",
      "java.net.InetAddress", "java.net.Inet4Address", "java.net.Inet6Address", "java.net.InetSocketAddress", "java.net.SocketAddress",
      "java.security.KeyRep", "java.security.cert.Certificate$CertificateRep");

  /** Classes used in published deserialization gadget chains; rejected even if an allow pattern matches. */
  public static final List<String> DEFAULT_DENY = List.of(
      "org.apache.commons.collections.functors.**", "org.apache.commons.collections4.functors.**",
      "org.apache.commons.beanutils.**", "org.apache.commons.fileupload.**", "org.apache.xalan.**",
      "com.sun.org.apache.xalan.**", "org.codehaus.groovy.runtime.**", "groovy.lang.**",
      "org.springframework.beans.factory.**", "org.springframework.aop.**",
      "org.springframework.core.SerializableTypeWrapper*", "org.hibernate.**", "org.jboss.**",
      "com.mchange.**", "bsh.**", "org.python.**", "org.mozilla.javascript.**", "clojure.**",
      "com.sun.rowset.**", "com.sun.jndi.**", "javax.naming.**", "javax.management.**",
      "javax.script.**", "java.rmi.**", "sun.rmi.**", "java.beans.**", "java.lang.invoke.**",
      "java.lang.reflect.**", "java.lang.Runtime", "java.lang.ProcessBuilder*", "java.net.URL",
      "java.security.SignedObject", "javax.xml.**", "com.sun.**", "sun.**", "jdk.**");

  /** Thrown (and wrapped in an InvalidClassException by ObjectInputStream) when the filter rejects input. */
  public static class RejectedClassException extends SecurityException {
    private static final long serialVersionUID = 1L;
    public RejectedClassException(String message) {
      super(message);
    }
  }

  private static final List<String> GLOBAL_ALLOW = new CopyOnWriteArrayList<String>();
  private static final JavaSerializationFilter DEFAULT = new JavaSerializationFilter(Collections.<String>emptyList(),
      DEFAULT_MAX_DEPTH, DEFAULT_MAX_REFS, DEFAULT_MAX_BYTES, DEFAULT_MAX_ARRAY);

  /** cache for {@link #forParameters(Parameters)}, keyed on the parameter values it was built from */
  private static volatile Object[] cache = new Object[2];

  private final List<String> allow;
  private final long maxDepth, maxRefs, maxBytes, maxArray;

  /**
   * @param extraAllow patterns allowed in addition to {@link #DEFAULT_ALLOW} (and {@link #allowGlobally})
   * @param maxDepth maximum graph depth, &lt;= 0 for no limit (likewise for the others)
   */
  public JavaSerializationFilter(List<String> extraAllow, long maxDepth, long maxRefs, long maxBytes, long maxArray) {
    List<String> a = new ArrayList<String>(DEFAULT_ALLOW);
    for (String p : extraAllow) a.add(validate(p));
    this.allow = Collections.unmodifiableList(a);
    this.maxDepth = maxDepth;
    this.maxRefs = maxRefs;
    this.maxBytes = maxBytes;
    this.maxArray = maxArray;
  }

  /**
   * JVM-wide extension point for applications: allows the given patterns in every FreePastry filter
   * (in addition to the {@value #PARAM_ALLOW} parameter).
   */
  public static void allowGlobally(String... patterns) {
    for (String p : patterns) GLOBAL_ALLOW.add(validate(p));
  }

  /** Removes patterns previously added with {@link #allowGlobally(String...)}. */
  public static void disallowGlobally(String... patterns) {
    for (String p : patterns) GLOBAL_ALLOW.remove(p);
  }

  /** The filter with built-in defaults (plus {@link #allowGlobally} patterns); used when no Environment is at hand. */
  public static JavaSerializationFilter getDefault() {
    return DEFAULT;
  }

  /** @return the filter configured by env's parameters, or {@code null} if the filter is disabled */
  public static JavaSerializationFilter forEnvironment(Environment env) {
    if (env == null) return DEFAULT;
    return forParameters(env.getParameters());
  }

  /** @return the filter configured by these parameters, or {@code null} if the filter is disabled */
  public static JavaSerializationFilter forParameters(Parameters p) {
    if (p == null) return DEFAULT;
    if (p.contains(PARAM_ENABLED) && !p.getBoolean(PARAM_ENABLED)) return null;
    String key = String.join("|", param(p, PARAM_ALLOW), param(p, PARAM_MAX_DEPTH), param(p, PARAM_MAX_REFS),
        param(p, PARAM_MAX_BYTES), param(p, PARAM_MAX_ARRAY));
    Object[] c = cache;
    if (key.equals(c[0])) return (JavaSerializationFilter) c[1];
    List<String> extra = new ArrayList<String>();
    for (String s : param(p, PARAM_ALLOW).split("[;,\\s]+")) {
      if (!s.isEmpty()) extra.add(s);
    }
    JavaSerializationFilter ret = new JavaSerializationFilter(extra,
        limit(p, PARAM_MAX_DEPTH, DEFAULT_MAX_DEPTH), limit(p, PARAM_MAX_REFS, DEFAULT_MAX_REFS),
        limit(p, PARAM_MAX_BYTES, DEFAULT_MAX_BYTES), limit(p, PARAM_MAX_ARRAY, DEFAULT_MAX_ARRAY));
    cache = new Object[] {key, ret};
    return ret;
  }

  /** Installs the filter configured by env (or the default when env is null) on the stream. */
  public static void install(ObjectInputStream ois, Environment env) {
    JavaSerializationFilter f = forEnvironment(env);
    if (f != null) ois.setObjectInputFilter(f);
  }

  private static String param(Parameters p, String name) {
    return p.contains(name) ? p.getString(name).trim() : "";
  }

  private static long limit(Parameters p, String name, long def) {
    String s = param(p, name);
    return s.isEmpty() ? def : Long.parseLong(s);
  }

  private static String validate(String pattern) {
    Objects.requireNonNull(pattern, "pattern");
    String p = pattern.trim();
    if (p.isEmpty() || p.startsWith("!") || p.contains("=") || p.contains("/"))
      throw new IllegalArgumentException("Invalid allow pattern '" + pattern
          + "', expected a class name, 'pkg.*', 'pkg.**' or 'prefix*'");
    return p;
  }

  /** true if className matches the jdk.serialFilter-style pattern */
  static boolean matches(String pattern, String className) {
    if (pattern.endsWith(".**")) {
      String pkg = pattern.substring(0, pattern.length() - 2); // keep the trailing '.'
      return className.startsWith(pkg);
    }
    if (pattern.endsWith(".*")) {
      String pkg = pattern.substring(0, pattern.length() - 1);
      return className.startsWith(pkg) && className.indexOf('.', pkg.length()) < 0;
    }
    if (pattern.endsWith("*")) {
      return className.startsWith(pattern.substring(0, pattern.length() - 1));
    }
    return className.equals(pattern);
  }

  private static boolean matchesAny(List<String> patterns, String className) {
    for (String p : patterns) {
      if (matches(p, className)) return true;
    }
    return false;
  }

  /** @return true if this filter (without the JVM-wide filter) admits the class */
  public boolean isAllowed(Class<?> clazz) {
    while (clazz.isArray()) clazz = clazz.getComponentType();
    if (clazz.isPrimitive()) return true;
    String name = clazz.getName();
    if (matchesAny(DEFAULT_DENY, name)) return false;
    if (matchesAny(allow, name) || matchesAny(GLOBAL_ALLOW, name)) return true;
    // exceptions and enums from the JDK carry no gadget behavior, and appear e.g. in PAST responses
    return name.startsWith("java.") && (Throwable.class.isAssignableFrom(clazz) || clazz.isEnum());
  }

  public Status checkInput(FilterInfo info) {
    check(maxDepth, info.depth(), PARAM_MAX_DEPTH, "graph depth");
    check(maxRefs, info.references(), PARAM_MAX_REFS, "number of object references");
    check(maxBytes, info.streamBytes(), PARAM_MAX_BYTES, "number of stream bytes");
    check(maxArray, info.arrayLength(), PARAM_MAX_ARRAY, "array length");

    Class<?> clazz = info.serialClass();
    if (clazz == null) return Status.UNDECIDED; // a limits-only callback

    ObjectInputFilter jvmWide = ObjectInputFilter.Config.getSerialFilter();
    Status jvmStatus = (jvmWide == null) ? Status.UNDECIDED : jvmWide.checkInput(info);
    if (jvmStatus == Status.REJECTED) {
      throw new RejectedClassException("Java deserialization of " + clazz.getName()
          + " was rejected by the JVM-wide filter (jdk.serialFilter).");
    }
    if (isAllowed(clazz) || (jvmStatus == Status.ALLOWED && !isDenied(clazz))) return Status.ALLOWED;

    Class<?> c = clazz;
    while (c.isArray()) c = c.getComponentType();
    String name = c.getName();
    if (isDenied(clazz)) {
      throw new RejectedClassException("Java deserialization of " + name + " was rejected by FreePastry's "
          + "deserialization filter: the class is on the deny list of known gadget classes. It cannot be allowed.");
    }
    String pkg = name.lastIndexOf('.') > 0 ? name.substring(0, name.lastIndexOf('.')) + ".**" : name;
    throw new RejectedClassException("Java deserialization of " + name + " was rejected by FreePastry's "
        + "deserialization filter: the class is not on the allowlist. If your application legitimately puts "
        + "this class into messages or storage, allow it with the parameter '" + PARAM_ALLOW + " = " + name
        + "' (or a pattern such as '" + pkg + "'), or call JavaSerializationFilter.allowGlobally(\"" + pkg + "\").");
  }

  private static boolean isDenied(Class<?> clazz) {
    while (clazz.isArray()) clazz = clazz.getComponentType();
    return !clazz.isPrimitive() && matchesAny(DEFAULT_DENY, clazz.getName());
  }

  private static void check(long max, long actual, String param, String what) {
    if (max > 0 && actual > max) {
      throw new RejectedClassException("Java deserialization rejected: " + what + " " + actual
          + " exceeds the limit of " + max + ". Raise the parameter '" + param + "' if this input is legitimate.");
    }
  }

  public String toString() {
    return "JavaSerializationFilter[allow=" + allow + ", global=" + GLOBAL_ALLOW + ", maxdepth=" + maxDepth
        + ", maxrefs=" + maxRefs + ", maxbytes=" + maxBytes + ", maxarray=" + maxArray + "]";
  }
}
