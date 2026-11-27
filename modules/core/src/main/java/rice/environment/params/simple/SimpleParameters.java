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
package rice.environment.params.simple;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.environment.params.ParameterChangeListener;
import rice.environment.params.Parameters;
import rice.p2p.util.PrivateFiles;

/**
 * A layered key/value parameter store: read-only defaults loaded from
 * {@code .params} files on the classpath, overlaid by an optional mutable
 * {@code .params} file that set*() changes and store() writes back.
 *
 * @author Alan Mislove
 */
public class SimpleParameters implements Parameters {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleParameters.class);


  private MyProperties properties;

  private MyProperties defaults;

  private Set<ParameterChangeListener> changeListeners;

  /**
   * Where items are written out.
   */
  private String configFileName;

  public static final String FILENAME_EXTENSION = ".params";

  public static final String ARRAY_SPACER = ",";

  public static final String defaultParamsFile = "user"+FILENAME_EXTENSION;

  /**
   *
   * @param orderedDefaults the names (without extension) of the default parameter
   *          files to load from the classpath, in order; later files override earlier ones
   * @param mutableConfigFileName the name (without extension) of the mutable parameter file;
   *          if this is null, no file is read and no params are saved (there is no implicit
   *          {@code user.params} in the working directory). If this file doesn't exist,
   *          you will get a warning printed to stdErr, then the file will be
   *          created if you ever store
   * @throws ParamsNotPresentException if a default file cannot be loaded
   */
  public SimpleParameters(String[] orderedDefaults, @Nullable String mutableConfigFileName) {
    // only a file that the caller names overrides the defaults: a user.params that happens to be in the working
    // directory is not read, as it could change reflectively loaded classes (nat_handler_class) of any program started there
    if (mutableConfigFileName != null) {
      this.configFileName = mutableConfigFileName + FILENAME_EXTENSION;
    }
    this.properties = new MyProperties();
    this.defaults = new MyProperties();
    this.changeListeners = new HashSet<ParameterChangeListener>();

    for (String orderedDefault : orderedDefaults) {
      try {
        ClassLoader loader = this.getClass().getClassLoader();
        // some VMs report the bootstrap classloader via null-return
        if (loader == null)
          loader = ClassLoader.getSystemClassLoader();
        try (InputStream inputStream = loader.getResource(
            orderedDefault + FILENAME_EXTENSION).openStream()) {
          this.defaults.load(inputStream);
        }
      } catch (Exception ioe) {
        String errorString = "Warning, couldn't load param file:"
          + (orderedDefault + FILENAME_EXTENSION);
        throw new ParamsNotPresentException(errorString,ioe);
      }
    }

    if (this.configFileName != null) {
      Path file = Path.of(this.configFileName);
      if (Files.exists(file)) {
        try (InputStream in = Files.newInputStream(file)) {
          properties.load(in);
        } catch (IOException | IllegalArgumentException e) {
          throw new ParamsNotPresentException("Error loading "+file, e);
        }
      } else {
        LOG.info("Configuration file {} not present.  Using defaults.", file.toAbsolutePath());
      }
    }
  }

  public Enumeration<Object> enumerateDefaults() {
    return defaults.keys();
  }

  public Enumeration<Object> enumerateNonDefaults() {
    return properties.keys();
  }

  protected @Nullable InetSocketAddress parseInetSocketAddress(String name)
      throws UnknownHostException {
    String host = name.substring(0, name.indexOf(":"));
    String port = name.substring(name.indexOf(":") + 1);

    try {
      return new InetSocketAddress(InetAddress.getByName(host), Integer
          .parseInt(port));
    } catch (UnknownHostException uhe) {
      LOG.error("Unable to find IP for ISA {} - returning null.", name);
      return null;
    }
  }

  protected @Nullable String getProperty(String name) {
    String result = properties.getProperty(name);

    if (result == null)
      result = defaults.getProperty(name);

    if (result == null) {
      LOG.warn("The parameter '{}' was not found - this is likely going to cause an error.", name);
    } else {
      // remove any surrounding whitespace
      result = result.trim();
    }

    return result;
  }

  /**
   * Note, this method does not implicitly call store()
   *
   * @see #store()
   */
  protected void setProperty(String name, String value) {
    if ((defaults.getProperty(name) != null)
        && defaults.getProperty(name).equals(value)) {
      // setting property back to default, remove override property if any
      if (properties.getProperty(name) != null) {
        properties.remove(name);
        fireChangeEvent(name, value);
      }
    } else {
      if ((properties.getProperty(name) == null)
          || !properties.getProperty(name).equals(value)) {
        properties.setProperty(name, value);
        fireChangeEvent(name, value);
      }
    }
  }

  @Override
  public void remove(String name) {
    properties.remove(name);
    fireChangeEvent(name, null);
  }

  @Override
  public boolean contains(String name) {
    if (defaults.containsKey(name))
      return true;
    return properties.containsKey(name);
  }

  @Override
  public int getInt(String name) {
    try {
      return Integer.parseInt(getProperty(name));
    } catch (NumberFormatException nfe) {
      throw new NumberFormatException(nfe.getMessage() + " for parameter "
          + name);
    }
  }

  @Override
  public double getDouble(String name) {
    try {
      return Double.parseDouble(getProperty(name));
    } catch (NumberFormatException nfe) {
      throw new NumberFormatException(nfe.getMessage() + " for parameter "
          + name);
    }
  }

  @Override
  public float getFloat(String name) {
    try {
      return Float.parseFloat(getProperty(name));
    } catch (NumberFormatException nfe) {
      throw new NumberFormatException(nfe.getMessage() + " for parameter "
          + name);
    }
  }

  @Override
  public long getLong(String name) {
    try {
      return Long.parseLong(getProperty(name));
    } catch (NumberFormatException nfe) {
      throw new NumberFormatException(nfe.getMessage() + " for parameter "
          + name);
    }
  }

  @Override
  public boolean getBoolean(String name) {
    return Boolean.valueOf(getProperty(name)).booleanValue();
  }

  @Override
  public InetAddress getInetAddress(String name) throws UnknownHostException {
    return InetAddress.getByName(getString(name));
  }

  @Override
  public @Nullable InetSocketAddress getInetSocketAddress(String name)
      throws UnknownHostException {
    return parseInetSocketAddress(requireString(name));
  }

  @Override
  @SuppressWarnings("StringSplitter") // dropping trailing empty entries is intended
  public InetSocketAddress[] getInetSocketAddressArray(String name)
      throws UnknownHostException {
    String list = requireString(name);
    if (list.length() == 0)
      return new InetSocketAddress[0];

    String[] addresses = list.split(ARRAY_SPACER);
    List<InetSocketAddress> result = new ArrayList<InetSocketAddress>();

    for (String string : addresses) {
      InetSocketAddress address = parseInetSocketAddress(string);

      if (address != null)
        result.add(address);
    }

    return (InetSocketAddress[]) result.toArray(new InetSocketAddress[0]);
  }

  private String requireString(String name) {
    return Objects.requireNonNull(getString(name), () -> "The parameter '" + name + "' is not set");
  }

  @Override
  public @Nullable String getString(String name) {
    return getProperty(name);
  }

  @Override
  public String @Nullable [] getStringArray(String name) {
    String list = getProperty(name);

    if (list != null)
      return (list.equals("") ? new String[0] : list.split(ARRAY_SPACER));
    else
      return null;
  }

  @Override
  public void setInt(String name, int value) {
    setProperty(name, Integer.toString(value));
  }

  @Override
  public void setDouble(String name, double value) {
    setProperty(name, Double.toString(value));
  }

  @Override
  public void setFloat(String name, float value) {
    setProperty(name, Float.toString(value));
  }

  @Override
  public void setLong(String name, long value) {
    setProperty(name, Long.toString(value));
  }

  @Override
  public void setBoolean(String name, boolean value) {
    setProperty(name, "" + value);
  }

  @Override
  public void setInetAddress(String name, InetAddress value) {
    setProperty(name, value.getHostAddress());
  }

  @Override
  public void setInetSocketAddress(String name, InetSocketAddress value) {
    setProperty(name, value.getAddress().getHostAddress() + ":"
        + value.getPort());
  }

  @Override
  public void setInetSocketAddressArray(String name, InetSocketAddress[] value) {
    StringBuilder buffer = new StringBuilder();

    for (int i = 0; i < value.length; i++) {
      buffer.append(value[i].getAddress().getHostAddress() + ":"
          + value[i].getPort());
      if (i < value.length - 1)
        buffer.append(ARRAY_SPACER);
    }

    setProperty(name, buffer.toString());
  }

  @Override
  public void setString(String name, String value) {
    setProperty(name, value);
  }

  @Override
  public void setStringArray(String name, String[] value) {
    StringBuilder buffer = new StringBuilder();

    for (int i = 0; i < value.length; i++) {
      buffer.append(value[i]);
      if (i < value.length - 1)
        buffer.append(ARRAY_SPACER);
    }

    setProperty(name, buffer.toString());
  }

  @Override
  public void store() throws IOException {
    if (configFileName == null)
      return;
    try {
      Path current = Path.of(configFileName);
      Path next = Path.of(configFileName + ".new");
      Path old = Path.of(configFileName + ".old");

      try (OutputStream out = PrivateFiles.newOutputStream(next)) {
        properties.store(out, null);
      }

      // as with renameTo and delete, a failed step is ignored
      moveIgnoringFailure(current, old);
      moveIgnoringFailure(next, current);
      try {
        Files.deleteIfExists(old);
      } catch (IOException ignored) {
        // as with delete, a failed step is ignored
      }
    } catch (IOException ioe) {
      LOG.error("Unable to store properties file {}", configFileName, ioe);
      throw ioe;
    }
  }

  private static void moveIgnoringFailure(Path from, Path path) {
    try {
      Files.move(from, path, REPLACE_EXISTING);
    } catch (IOException ignored) {
      // as with renameTo, a failed move is ignored
    }
  }

  @Override
  public void addChangeListener(ParameterChangeListener parameterChangeListener) {
    changeListeners.add(parameterChangeListener);
  }

  @Override
  public void removeChangeListener(ParameterChangeListener parameterChangeListener) {
    changeListeners.remove(parameterChangeListener);
  }

  private void fireChangeEvent(String name, @Nullable String value) {
    for (ParameterChangeListener parameterChangeListener : changeListeners) {
      parameterChangeListener.parameterChange(name, value);
    }
  }

  protected static class MyProperties extends Properties {
    @Override
    public synchronized Enumeration<Object> keys() {
      final String[] keys = (String[]) keySet().toArray(new String[0]);
      Arrays.sort(keys);

      return new Enumeration<Object>() {
        int pos = 0;

        @Override
        public boolean hasMoreElements() {
          return (pos < keys.length);
        }

        @Override
        public Object nextElement() {
          return keys[pos++];
        }
      };
    }
  }
}
