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

package rice.p2p.multiring;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Locale;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.util.SecurityUtils;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.dist.DistPastryNodeFactory;

/**
 * This class represents a certificate for a specific ring attached, which
 * contains informtation about the ring, such as bootstraps, etc...
 *
 * @version $Id: RingCertificate.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
public class RingCertificate {
  private static final Logger LOG = LoggerFactory.getLogger(RingCertificate.class);


  // the format version of certificate files
  private static final int CERTIFICATE_FORMAT_VERSION = 1;

  // the static map of all RingCertificates available (id -> cert)
  private static final HashMap<Id, RingCertificate> CERTIFICATES = new HashMap<Id, RingCertificate>();

  // load all certificates that can be found
  static {
    try {
      // first load the CA public key
      URL a = ClassLoader.getSystemResource("ca.publickey");
      PublicKey caPublic;
      try (InputStream b = a.openStream()) {
        caPublic = SecurityUtils.decodePublicKey(readGzippedBytes(b));
      }

      // and finally load and verify the certs
      URL a1 = ClassLoader.getSystemResource("ringcert.list");
      InputStream b1 = a1.openStream();
      InputStreamReader c1 = new InputStreamReader(b1, StandardCharsets.UTF_8);
      BufferedReader r = new BufferedReader(c1);
      String filename = null;

      while ((filename = r.readLine()) != null) {
        URL a2 = ClassLoader.getSystemResource(filename);
        InputStream b2 = a2.openStream();
        RingCertificate cert = readFromStream(b2);

        if (cert.verify(caPublic)) {
          if ((getCertificate(cert.getId()) == null) ||
              (getCertificate(cert.getId()).getVersion() < cert.getVersion())) {
            cert.refresh();
            CERTIFICATES.put(cert.getId(), cert);
          }
        } else {
          LOG.warn("RINGCERT: Could not verify ring certificate {} ignoring.", cert);
        }
      }
    } catch (Exception e) {
      LOG.error("RINGCERT: Found an exception while reading in ring certificates!", e);
    }
  }

  /**
   * Method which returns the certificates found for the given ringId
   *
   * @param ringId The id
   * @return All certificate
   */
  public static RingCertificate getCertificate(Id ringId) {
    return (RingCertificate) CERTIFICATES.get(ringId);
  }

  /**
   * Refreshes all of the InetAddresses based on their name, not IP address.
   *
   * In other words, it fixes InetAddresses that are stale from storage.  It reads
   * the "name" of the InetAddress, and then builds a new InetAddress with this name
   * which should do a proper dns lookup.
   *
   */
  private void refresh() {
    logServer = refreshAddress(logServer);
  }

  /**
   * Helper function for refresh().  Returns an identical InetSocketAddress unless
   * the ip address in address is stale.  Then it returns the current one based on a
   * dns lookup.
   */
  private InetSocketAddress refreshAddress(InetSocketAddress address) {
    return new InetSocketAddress(address.getAddress().getHostName(), address.getPort());
  }

  // the name of this ring
  private final String name;

  // the Id of this ring
  private final Id id;

  // the protocol of this ring
  private final int protocol;

  // the list of bootstrap nodes for this ring
  private final InetSocketAddress[] bootstraps;

  // the preferred port for this ring
  private final int port;

  // the logserver for this ring
  private InetSocketAddress logServer;

  // the key for the ring, used in visualization and log uploading
  private final PublicKey key;

  // the version number of this certificate
  private final long version;

  // the signature of the above info
  private byte[] signature;

  /**
   * Builds a new RingCertificate given the appropriate info
   *
   */
  public RingCertificate(String name, Id id, int protocol, InetSocketAddress[] bootstraps, int port, PublicKey key, InetSocketAddress logServer) {
    this.name = name;
    this.id = id;
    this.bootstraps = bootstraps;
    this.port = port;
    this.key = key;
    this.logServer = logServer;
    this.version = System.currentTimeMillis();
    this.protocol = protocol;
  }

  /**
   * Returns the name of this ring
   *
   * @return The name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the id of this ring
   *
   * @return The id
   */
  public Id getId() {
    return id;
  }

  /**
    * Returns the protcol of this ring
   *
   * @return The protocol
   */
  public int getProtocol() {
    return protocol;
  }

  /**
   * Returns the version of this ring cert
   *
   * @return The version
   */
  public long getVersion() {
    return version;
  }

  /**
   * Returns the bootstraps of this ring
   *
   * @return The bootstraps
   */
  public InetSocketAddress[] getBootstraps() {
    return bootstraps;
  }

  /**
   * Returns the preferred port of this ring
   *
   * @return The preferred port
   */
  public int getPort() {
    return port;
  }

  /**
   * Returns the public key which is used to authenticate
   *
   * @return The public key
   */
  public PublicKey getKey() {
    return key;
  }

  /**
   * Returns the log server of this ring
   *
   * @return The log server
   */
  public InetSocketAddress getLogServer() {
    return logServer;
  }

  /**
   * Signs this RingCertificate, given the private key to sign
   * with
   *
   * @param priv The private key to sign with
   */
  void sign(PrivateKey priv) {
    if (signature != null)
      throw new IllegalArgumentException("Attempt to sign an already-signed RingCertificate!");

    try {
      signature = SecurityUtils.sign(getIdentifier(), priv);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Verifies this RingCertificate, given the public key to verify
   * with
   *
   * @param pub The public key to verify with
   */
  boolean verify(PublicKey pub) {
    if (signature == null)
      throw new IllegalArgumentException("Attempt to verify an unsigned RingCertificate!");

    try {
      return SecurityUtils.verify(getIdentifier(), signature, pub);
    } catch (SecurityException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a string of this object
   *
   * @return a string
   */
  @Override
  public String toString() {
    return "[Ring Certificate for ring '" + name + "' (" + id + ")]";
  }

  /**
   * Writes this certificate to the given file
   *
   * @param file The file to write to
   */
  private void writeToFile(Path file) throws IOException {
    try (OutputStream out = Files.newOutputStream(file)) {
      writeToStream(out);
    }
  }

  /**
   * Writes this certificate, gzipped: the format version, the signed fields (see {@link #getIdentifier()})
   * and the signature.
   *
   * @param stream The stream to write to
   */
  void writeToStream(OutputStream stream) throws IOException {
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(stream)));
    out.writeInt(CERTIFICATE_FORMAT_VERSION);
    writeFields(out);
    out.writeInt(signature.length);
    out.write(signature);
    out.close();
  }

  /**
   * Reads a certificate from the given stream
   *
   * @param stream The stream to read from
   */
  static RingCertificate readFromStream(InputStream stream) throws IOException {
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(new GZIPInputStream(stream)))) {
      int version = in.readInt();
      if (version != CERTIFICATE_FORMAT_VERSION) {
        throw new IOException("Unknown ring certificate format " + version);
      }
      return new RingCertificate(in);
    }
  }

  /**
   * Reading constructor, the counterpart of {@link #writeFields(DataOutput)} followed by the signature.
   */
  private RingCertificate(DataInput in) throws IOException {
    name = in.readUTF();
    id = rice.pastry.Id.build(readBytes(in));
    protocol = in.readInt();
    port = in.readInt();
    version = in.readLong();
    bootstraps = new InetSocketAddress[in.readInt()];
    for (int i = 0; i < bootstraps.length; i++) {
      bootstraps[i] = readAddress(in);
    }
    logServer = in.readBoolean() ? readAddress(in) : null;
    key = SecurityUtils.decodePublicKey(readBytes(in));
    signature = readBytes(in);
  }

  private void writeFields(DataOutput out) throws IOException {
    out.writeUTF(name);
    writeBytes(out, id.toByteArray());
    out.writeInt(protocol);
    out.writeInt(port);
    out.writeLong(version);
    out.writeInt(bootstraps.length);
    for (InetSocketAddress bootstrap : bootstraps) {
      writeAddress(out, bootstrap);
    }
    out.writeBoolean(logServer != null);
    if (logServer != null) {
      writeAddress(out, logServer);
    }
    writeBytes(out, SecurityUtils.encodePublicKey(key));
  }

  private static void writeAddress(DataOutput out, InetSocketAddress address) throws IOException {
    out.writeUTF(address.getHostString());
    out.writeInt(address.getPort());
  }

  private static InetSocketAddress readAddress(DataInput in) throws IOException {
    String host = in.readUTF();
    return new InetSocketAddress(host, in.readInt());
  }

  private static void writeBytes(DataOutput out, byte[] bytes) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static byte[] readBytes(DataInput in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return bytes;
  }

  /**
   * Writes a byte array to a file, gzipped and preceded by its length.
   */
  private static void writeGzippedBytes(Path file, byte[] bytes) throws IOException {
    try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(Files.newOutputStream(file))))) {
      writeBytes(out, bytes);
    }
  }

  /**
   * Reads a byte array written by {@link #writeGzippedBytes(Path, byte[])}.
   */
  static byte[] readGzippedBytes(InputStream stream) throws IOException {
    DataInputStream in = new DataInputStream(new BufferedInputStream(new GZIPInputStream(stream)));
    return readBytes(in);
  }

  /**
   * Internal method which writes out the keypair to a file, encrypted
   *
   * @param pair The keypair
   * @param pass THe password
   * @param ring The ring name
   */
  private static void writeKeyPair(KeyPair pair, String pass, String ring) throws IOException {
    byte[] cipher = SecurityUtils.encryptWithPassword(SecurityUtils.encodeKeyPair(pair), pass.toCharArray());
    writeGzippedBytes(Path.of(ring.toLowerCase(Locale.ROOT) + ".ringkeypair.enc"), cipher);
  }

  /**
   * Internal method which writes out the keypair to a file, encrypted
   *
   * @param pass THe password
   * @param ring The ring name
   */
  public static KeyPair readKeyPair(String ring, String pass) throws IOException {
    byte[] cipher;
    try (InputStream in = Files.newInputStream(Path.of(ring.toLowerCase(Locale.ROOT) + ".ringkeypair.enc"))) {
      cipher = readGzippedBytes(in);
    }

    return SecurityUtils.decodeKeyPair(SecurityUtils.decryptWithPassword(cipher, pass.toCharArray()));
  }

  /**
   * Internal method which returns the to-be signed data
   *
   * @return The data
   */
  private byte[] getIdentifier() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      writeFields(out);
    }
    return bytes.toByteArray();
  }

  /**
   * Main method which, as a utility, generates a RingCertificate by asking the user
   * for prompts
   *
   */
  public static void main(String[] args) throws Exception {
    Environment env = new Environment();
    BufferedReader r = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

    // first, load the CA keypair
    Path f = Path.of("ca.keypair.enc");
    byte[] caCipher;
    try (InputStream in = Files.newInputStream(f)) {
      caCipher = readGzippedBytes(in);
    }
    KeyPair caPair = SecurityUtils.decodeKeyPair(SecurityUtils.decryptWithPassword(caCipher, prompt(r, w, "Please enter the CA password: ").trim().toCharArray()));

    // get the ring info
    String ring = prompt(r, w, "Please enter the name of the ring (rice, berkeley): ");
    @SuppressWarnings("StringSplitter") // dropping trailing empty entries is intended
    String[] bootstrap = prompt(r, w, "Please enter the bootstraps (host1:port1,host2:port2...): ").trim().split(",");
    int port = Integer.parseInt(prompt(r, w, "Please enter the default port for nodes: "));
    String logServer = prompt(r, w, "Please enter the log upload server (host:port): ");
    String pass = prompt(r, w, "Please enter a password for the ring keypair: ");

    // socket is the only protocol
    int protocolId = DistPastryNodeFactory.PROTOCOL_SOCKET;

    // build the id
    Id id = generateId(ring, env);

    // translate the InetSocketAddresses
    InetSocketAddress log = toInetSocketAddress(logServer);
    InetSocketAddress[] bootstraps = new InetSocketAddress[bootstrap.length];
    for (int i=0; i<bootstraps.length; i++)
      bootstraps[i] = toInetSocketAddress(bootstrap[i]);

    // generate a keypair
    KeyPair pair = SecurityUtils.generateKeyAsymmetric();

    // now create the Ring Certificate
    RingCertificate cert = new RingCertificate(ring, id, protocolId, bootstraps, port, pair.getPublic(), log);
    cert.sign(caPair.getPrivate());
    if (! cert.verify(caPair.getPublic()))
      throw new RuntimeException("Could not verify generated certificate!");

    cert.writeToFile(Path.of(ring.toLowerCase(Locale.ROOT) + ".ringcert"));

    // and finally write out the KeyPair
    writeKeyPair(pair, pass, ring.toLowerCase(Locale.ROOT));

    // exit explicitly, the Environment's threads would keep the JVM running
    System.exit(0);
  }

  /**
   * Internal method for prompting the user
   *
   * @param r The reader
   * @param w The writer
   * @param prompt The prompt
   * @return The result
   */
  private static String prompt(BufferedReader r, BufferedWriter w, String prompt) throws IOException {
    w.write(prompt);
    w.flush();
    return r.readLine();
  }

  /**
   * Internal method for String -&gt; InetSocketAddress
   *
   * @param s The string, as host:port
   * @return The address
   */
  private static InetSocketAddress toInetSocketAddress(String s) throws IOException {
    String host = s.substring(0, s.indexOf(":"));
    int port = Integer.parseInt(s.substring(s.indexOf(":") + 1));

    return new InetSocketAddress(host, port);
  }

  /**
   * Internal method which builds the ring id from the canonical form of the ring name
   *
   * @param s The ring name
   * @param env The environment
   * @return The ring id
   */
  private static Id generateId(String s, Environment env) {
    String ring = s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1).toLowerCase(Locale.ROOT);

    PastryIdFactory pif = new PastryIdFactory(env);

    Id ringId = pif.buildId(ring);
    byte[] ringData = ringId.toByteArray();

    int base = MultiringConfig.fromParameters(env.getParameters()).base();
    for (int i=0; i<ringData.length - base; i++)
      ringData[i] = 0;


    if (s.toLowerCase(Locale.ROOT).equals("global"))
      for (int i=0; i<ringData.length; i++)
        ringData[i] = 0;

    return pif.buildId(ringData);
  }
}
