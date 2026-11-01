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

package rice.pastry.standard;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import rice.environment.Environment;
import rice.p2p.util.SecurityUtils;
import rice.pastry.Id;
import rice.pastry.NodeIdFactory;

/**
 * Builds nodeIds in a certified manner, guaranteeing that a given node will always
 * have the same nodeId.  NOTE:  Actual certification is not yet implemented, rather,
 * using this factory simply guarantees that the node's nodeId will never change.
 *
 * @version $Id: CertifiedNodeIdFactory.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class CertifiedNodeIdFactory implements NodeIdFactory {

  public static final String NODE_ID_FILENAME = "nodeId";

  // the format version of node id certificates
  private static final int CERTIFICATE_FORMAT_VERSION = 1;

  private final int port;

  private final Environment environment;

  private final Logger logger;

  /**
   * Constructor.
   */
  public CertifiedNodeIdFactory(InetAddress localIP, int port, Environment env) {
    this.environment = env;
    this.port = port;
    this.logger = environment.getLogManager().getLogger(CertifiedNodeIdFactory.class,null);
    new IPNodeIdFactory(localIP, port, env); // logs an alert if localIP is a loopback address
  }

  /**
   * generate a nodeId
   *
   * @return the new nodeId
   */
  @Override
  public Id generateNodeId() {
    Path f = Path.of(NODE_ID_FILENAME);

    if (! Files.exists(f)) {
      Path g = Path.of("." + NODE_ID_FILENAME + "-" + port);

      if (Files.exists(g)) {
        try {
          Files.move(g, f, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
          // as with renameTo, a failed rename shows up as the missing certificate below
        }
      }
    }

    if (! Files.exists(f)) {
      logger.warn("Unable to find NodeID certificate - exiting.");
      throw new RuntimeException("Unable to find NodeID certificate - make sure that the NodeID certificate file '" + NODE_ID_FILENAME + "' exists in your ePOST directory.");
    }

    try (InputStream in = Files.newInputStream(f)) {
      return readCertificate(in);
    } catch (IOException e) {
      logger.warn("", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads the node id from a certificate written by {@link #generateCertificate(Id, OutputStream, PrivateKey)}.
   * The signature is not checked.
   *
   * @param in the certificate
   * @return the node id
   */
  static Id readCertificate(InputStream in) throws IOException {
    DataInputStream data = new DataInputStream(in);
    int version = data.readInt();
    if (version != CERTIFICATE_FORMAT_VERSION) {
      throw new IOException("Unknown node id certificate format " + version);
    }
    byte[] id = new byte[data.readInt()];
    data.readFully(id);
    return Id.build(id);
  }

  /**
   * Method which generates a certificate given the nodeid, location, and private key: the format version,
   * the id's bytes and the signature over them, each preceded by its length.
   *
   * @param id The id of the certificate to generate
   * @param os The stream to write the certificate to
   * @param key The private key to use to sign the result
   */
  public static void generateCertificate(Id id, OutputStream os, PrivateKey key) {
    try (DataOutputStream out = new DataOutputStream(os)) {
      byte[] bytes = id.toByteArray();
      byte[] signature = SecurityUtils.sign(bytes, key);

      out.writeInt(CERTIFICATE_FORMAT_VERSION);
      out.writeInt(bytes.length);
      out.write(bytes);
      out.writeInt(signature.length);
      out.write(signature);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Main method which, for convenience, allows certificate creation.  The parameters allowed are
   * -ca [file] -out [dir]
   */
  public static void main(String[] args) throws Exception {
    String pw = getArg(args, "-pw");
    String caDirectory = getArg(args, "-ca");

    // the CA key pair, encrypted with the password: gzipped, preceded by its length
    Path f = (caDirectory == null) ? Path.of("ca.keypair.enc") : Path.of(caDirectory, "ca.keypair.enc");
    byte[] cipher;
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(new GZIPInputStream(Files.newInputStream(f))))) {
      cipher = new byte[in.readInt()];
      in.readFully(cipher);
    }

    KeyPair caPair = SecurityUtils.decodeKeyPair(SecurityUtils.decryptWithPassword(cipher, pw.toCharArray()));

    Environment env = new Environment();
    generateCertificate(new RandomNodeIdFactory(env).generateNodeId(), Files.newOutputStream(Path.of(NODE_ID_FILENAME)), caPair.getPrivate());
    env.destroy();
  }

  public static String getArg(String[] args, String argType) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith(argType)) {
        if (args.length > i+1) {
          String ret = args[i+1];
          if (!ret.startsWith("-"))
            return ret;
        }
      }
    }
    return null;
  }
}
