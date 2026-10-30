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
package rice.environment.logging;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default {@link LogManager}: creates loggers through {@link LoggerFactory}.
 *
 * The logger name is the fully qualified class name, followed by {@code "." + instance} when a non-empty instance
 * name is given, followed by {@code "." + prefix} when this manager has a prefix. Because every name starts with the class
 * name, the usual SLF4J backend configuration by class or package prefix applies, for example
 * {@code org.slf4j.simpleLogger.log.rice.pastry.socket=debug}.
 *
 * Node factories give each node of a multi-node JVM its own manager with {@link #clone(String)}, so that the
 * loggers of different nodes can be told apart (and configured separately) by their name suffix.
 */
public class Slf4jLogManager implements LogManager {

  private final String prefix;

  /** A manager without a prefix. */
  public Slf4jLogManager() {
    this(null);
  }

  /**
   * @param prefix appended to every logger name, usually a node id; null or empty for none
   */
  public Slf4jLogManager(@Nullable String prefix) {
    this.prefix = (prefix == null) ? "" : prefix;
  }

  @Override
  public Logger getLogger(Class<?> clazz, @Nullable String instance) {
    return LoggerFactory.getLogger(loggerName(clazz, instance));
  }

  /**
   * @param clazz the class
   * @param instance the instance name, or null or empty for none
   * @return the name {@link #getLogger(Class, String)} uses: {@code class[.instance][.prefix]}
   */
  public String loggerName(Class<?> clazz, @Nullable String instance) {
    StringBuilder name = new StringBuilder(clazz.getName());
    if (instance != null && !instance.isEmpty()) name.append('.').append(instance);
    if (!prefix.isEmpty()) name.append('.').append(prefix);
    return name.toString();
  }

  /**
   * Returns a new manager whose loggers carry the given prefix instead of this manager's.
   *
   * @param newPrefix usually a node id
   * @return a new LogManager
   */
  public Slf4jLogManager clone(String newPrefix) {
    return new Slf4jLogManager(newPrefix);
  }

  /** @return the prefix appended to logger names, "" for none */
  public String getPrefix() {
    return prefix;
  }

  @Override
  public String toString() {
    return "Slf4jLogManager(" + prefix + ")";
  }
}
