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
package org.mpisws.p2p.pki.x509;

import java.io.Console;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.Map;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.GeneralSecurityException;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.security.spec.RSAKeyGenParameterSpec;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import javax.security.auth.x500.X500Principal;

import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectKeyIdentifier;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.p2p.util.SecurityUtils;
import rice.p2p.util.tuples.Tuple;
import rice.pastry.Id;
import rice.pastry.standard.NodeIdCertificate;

public class CertificateAuthorityTool {

  static final SecureRandom random = new SecureRandom();
  /** size of the random serial numbers of node certificates */
  private static final int SERIAL_BITS = 128;
  /** Was SHA1withRSA, which JDK 25 rejects for certificate paths in TLS. */
  public static final String DEFAULT_SIGNATURE_ALGORITHM = "SHA256withRSA";
  /** RSA modulus size for generated CA and node keys (was 768, which JDK 25 rejects in TLS). */
  public static final int DEFAULT_KEY_SIZE = 2048;
  /** Type of the key stores this tool reads and writes (was BouncyCastle's legacy "UBER"). */
  public static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

  /** Suffix of the issuance log next to the CA store {@code <name>-store}. */
  public static final String ISSUANCE_LOG_SUFFIX = "-issued.log";

  X509Certificate cert;
  KeyPair keyPair;
  /** where every certificate this CA issues is recorded, or null for none */
  Path issuanceLog;

  public CertificateAuthorityTool(X509Certificate cert, KeyPair caPair) {
    this.cert = cert;
    this.keyPair = caPair;
  }

  /**
   * Records every certificate this tool issues from now on, one line per certificate appended to file (created
   * readable by its owner only): issue time, serial (hex), id (the common name), subject and end of validity,
   * separated by tabs. {@link #getCATool} sets it to {@code <name>}{@value #ISSUANCE_LOG_SUFFIX}.
   */
  public void setIssuanceLog(Path file) {
    this.issuanceLog = file;
  }

  /** Appends issued to the issuance log; issuing fails if it cannot be recorded. */
  private X509Certificate recordIssuance(String commonName, X509Certificate issued) {
    if (issuanceLog == null) return issued;
    String line = String.join("\t", DateTimeFormatter.ISO_INSTANT.format(Instant.now()),
        "serial=" + issued.getSerialNumber().toString(16), "id=" + commonName,
        "subject=" + issued.getSubjectX500Principal().getName(),
        "notAfter=" + DateTimeFormatter.ISO_INSTANT.format(issued.getNotAfter().toInstant()));
    try {
      if (!Files.exists(issuanceLog)) newPrivateOutputStream(issuanceLog).close();
      Files.write(issuanceLog, List.of(line), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    } catch (IOException ioe) {
      throw new UncheckedIOException("Could not record the issued certificate in " + issuanceLog, ioe);
    }
    return issued;
  }

  public X509Certificate getCertificate() {
    return cert;
  }

  /**
   *
   * @param CN only used when generating a new CA
   * @param chars the password of the CA key store and of the CA key; must not be empty
   * @throws IllegalArgumentException if pw is empty
   */
  public static CertificateAuthorityTool getCATool(String CN, char[] chars) throws KeyStoreException, NoSuchProviderException, NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException, InvalidKeyException, IllegalStateException, SignatureException, InvalidAlgorithmParameterException {

    requirePassword(chars, "CA");
    X509Certificate caCert;
    KeyPair caPair;

    Path caStoreFile = Path.of(CN+"-store");
    if (Files.exists(caStoreFile)) {
      KeyStore store = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
      try (InputStream fis = Files.newInputStream(caStoreFile)) {
        store.load(fis, chars);
      } catch (IOException ioe) {
        throw new IOException("Could not load "+caStoreFile+" as a "+DEFAULT_KEYSTORE_TYPE+" key store: wrong password, or a store written by an older FreePastry (UBER format, no longer supported)", ioe);
      }
      caCert = (X509Certificate)store.getCertificate(CA_STORE_CERT);
      PrivateKey priv = (PrivateKey)store.getKey(CA_STORE_PRIVATE, chars);
      caPair = new KeyPair(caCert.getPublicKey(),priv);
      if (caCert.getVersion() < 3) {
        System.err.println("The CA certificate in "+caStoreFile+" is version 1 (no basicConstraints CA:true); re-issue it with -reissue-ca");
      }
    } else {
      // make a self-signed cert
      Instant exp = ZonedDateTime.now(ZoneOffset.UTC).plusYears(10).toInstant();
      Tuple<X509Certificate, KeyPair> pair = generateNewCA(CN, exp);
      caCert = pair.first();
      caPair = pair.second();

      System.out.println(caCert);

      writeCAStore(caStoreFile, caCert, caPair.getPrivate(), chars);
    }
    CertificateAuthorityTool tool = new CertificateAuthorityTool(caCert, caPair);
    tool.setIssuanceLog(Path.of(CN+ISSUANCE_LOG_SUFFIX));
    return tool;
  }

  private static void writeCAStore(Path caStoreFile, X509Certificate caCert, PrivateKey caKey, char[] chars) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
    // PKCS12 cannot hold a bare public key: it is read back from the certificate
    KeyStore store = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
    store.load(null, null);
    store.setKeyEntry(CA_STORE_PRIVATE, caKey, chars, new Certificate[] {caCert});
    store.setCertificateEntry(CA_STORE_CERT, caCert);

    try (OutputStream fos = newPrivateOutputStream(caStoreFile)) {
      store.store(fos, chars);
    }
  }

  /**
   * Replaces the CA certificate in the store {@code <CN>-store} with a version 3 one for the same key and subject
   * (see {@link #reissueCA}), valid for ten years from now. Node certificates issued before stay valid under it;
   * nodes need the new CA certificate in their key stores.
   *
   * @return the tool with the new CA certificate
   * @throws IOException if there is no such store
   */
  public static CertificateAuthorityTool reissueCATool(String CN, char[] chars) throws GeneralSecurityException, IOException {
    Path caStoreFile = Path.of(CN+"-store");
    if (!Files.exists(caStoreFile)) throw new IOException("No CA store "+caStoreFile);
    CertificateAuthorityTool old = getCATool(CN, chars);
    X509Certificate reissued = reissueCA(old.getCertificate(), old.keyPair, Instant.now(), ZonedDateTime.now(ZoneOffset.UTC).plusYears(10).toInstant());
    writeCAStore(caStoreFile, reissued, old.keyPair.getPrivate(), chars);
    CertificateAuthorityTool tool = new CertificateAuthorityTool(reissued, old.keyPair);
    tool.setIssuanceLog(Path.of(CN+ISSUANCE_LOG_SUFFIX));
    return tool;
  }

  /**
   * A version 3 CA certificate for an existing CA: same key, same subject and same serial number as caCertificate,
   * new validity period. Node certificates issued under caCertificate verify under the new one, also by PKIX path
   * building: their authority key identifier names the CA key by the SHA-1 hash of its encoded public key and the
   * old certificate's issuer and serial number, all of which the new certificate keeps.
   *
   * @param caKeys the CA's key pair; its public key must be the one in caCertificate
   */
  public static X509Certificate reissueCA(X509Certificate caCertificate, KeyPair caKeys, Instant startDate, Instant expiryDate) throws CertificateEncodingException, InvalidKeyException {
    if (!caCertificate.getPublicKey().equals(caKeys.getPublic())) {
      throw new InvalidKeyException("The key pair is not the one of the CA certificate "+caCertificate.getSubjectX500Principal());
    }
    return generateCA(caCertificate.getSubjectX500Principal(), startDate, expiryDate, caCertificate.getSerialNumber(), caKeys, DEFAULT_SIGNATURE_ALGORITHM);
  }

  public static Tuple<X509Certificate, KeyPair> generateNewCA(String CN, Instant expiryDate) throws CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException, InvalidAlgorithmParameterException {
    KeyPair caPair = generateKeyPair();

    X509Certificate cert = generateCA(new X500Principal("CN="+CN), Instant.now(), expiryDate, randomSerial(), caPair, DEFAULT_SIGNATURE_ALGORITHM);

    return new Tuple<X509Certificate, KeyPair>(cert,caPair);
  }

  /**
   * @return a new RSA key pair of {@link #DEFAULT_KEY_SIZE} bits, suitable for a CA or a node certificate
   */
  public static KeyPair generateKeyPair() throws NoSuchAlgorithmException, NoSuchProviderException, InvalidAlgorithmParameterException {
    KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA", SecurityUtils.BOUNCY_CASTLE);
    keyPairGen.initialize(new RSAKeyGenParameterSpec(DEFAULT_KEY_SIZE, RSAKeyGenParameterSpec.F4), random);
    return keyPairGen.generateKeyPair();
  }

  /**
   *
   * @param CN common name
   * @return the CA cert, see {@link #generateCA}
   */
  public static X509Certificate generateNewCA(String CN, Instant startDate, Instant expiryDate, long serialNumber, KeyPair keyPair, String signatureAlgorithm) throws CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    return generateCA(new X500Principal("CN="+CN), startDate, expiryDate, BigInteger.valueOf(serialNumber), keyPair, signatureAlgorithm);
  }

  /**
   * A self-signed version 3 CA certificate (was version 1, which strict PKIX consumers do not accept as a CA):
   * basicConstraints CA:true with path length 0 (it signs node certificates only), keyUsage keyCertSign and cRLSign,
   * both critical, and a subject key identifier, the SHA-1 hash of the encoded public key (the key identifier that
   * node certificates issued under the earlier version 1 CA certificate carry).
   */
  static X509Certificate generateCA(X500Principal dnName, Instant startDate, Instant expiryDate, BigInteger serialNumber, KeyPair keyPair, String signatureAlgorithm) throws CertificateEncodingException, InvalidKeyException {
    X509v3CertificateBuilder certGen = new JcaX509v3CertificateBuilder(
        dnName, serialNumber, Date.from(startDate), Date.from(expiryDate),
        dnName,                                         // note: same as issuer
        keyPair.getPublic());
    try {
      byte[] keyIdentifier = MessageDigest.getInstance("SHA-1").digest(keyPair.getPublic().getEncoded());
      certGen.addExtension(Extension.basicConstraints, true, new BasicConstraints(0));
      certGen.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
      certGen.addExtension(Extension.subjectKeyIdentifier, false, new SubjectKeyIdentifier(keyIdentifier));
    } catch (CertIOException | NoSuchAlgorithmException e) {
      throw new CertificateEncodingException(e);
    }

    return generate(certGen.build(signer(signatureAlgorithm, keyPair.getPrivate())));
  }

  /**
   * @param signatureAlgorithm e.g. DEFAULT_SIGNATURE_ALGORITHM
   * @param privateKey the key the certificate is signed with
   */
  private static ContentSigner signer(String signatureAlgorithm, PrivateKey privateKey) throws InvalidKeyException {
    try {
      return new JcaContentSignerBuilder(signatureAlgorithm).setProvider(SecurityUtils.BOUNCY_CASTLE).build(privateKey);
    } catch (OperatorCreationException oce) {
      throw new InvalidKeyException("Could not create signer for "+signatureAlgorithm, oce);
    }
  }

  private static X509Certificate generate(org.bouncycastle.cert.X509CertificateHolder holder) throws CertificateEncodingException {
    try {
      return new JcaX509CertificateConverter().setProvider(SecurityUtils.BOUNCY_CASTLE).getCertificate(holder);
    } catch (CertificateException certificateException) {
      throw new CertificateEncodingException(certificateException);
    }
  }

  public X509Certificate sign(String CN, PublicKey key) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    Instant exp = ZonedDateTime.now(ZoneOffset.UTC).plusYears(1).toInstant();
    return recordIssuance(CN, sign(CN,key,Instant.now(),exp,randomSerial(),cert,keyPair.getPrivate()));
  }

  /**
   * @return a node id the CA chooses: 160 bits from {@link SecureRandom}, so that a requester cannot place itself
   *     next to a key it wants to attack (was {@code RandomNodeIdFactory}, a {@code java.util.Random} seeded from
   *     the clock and the interface addresses)
   */
  public static Id randomNodeId() {
    return Id.makeRandomId(random);
  }

  /** Issues the certificate for a node id the CA chooses ({@link #randomNodeId()}), see {@link #signNodeId(Id, PublicKey)}. */
  public X509Certificate signRandomNodeId(PublicKey key) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    return signNodeId(randomNodeId(), key);
  }

  /**
   * Issues the certificate that binds a node id to a public key, valid for one year: the subject is
   * {@link NodeIdCertificate#subjectFor(Id)}, which peers and {@code CertifiedNodeIdFactory} check. The id is the
   * caller's choice: an operator who lets requesters choose it gives up Sybil resistance (a requester can pick an
   * id next to a target key); use {@link #signRandomNodeId} for ids nobody chooses.
   */
  public X509Certificate signNodeId(Id id, PublicKey key) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    Instant now = Instant.now();
    return signNodeId(id, key, now, ZonedDateTime.now(ZoneOffset.UTC).plusYears(1).toInstant());
  }

  /**
   * @param notBefore start of the validity period
   * @param notAfter end of the validity period
   */
  public X509Certificate signNodeId(Id id, PublicKey key, Instant notBefore, Instant notAfter) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    return recordIssuance(id.toStringFull(), sign(id.toStringFull(), key, notBefore, notAfter, randomSerial(), cert, keyPair.getPrivate()));
  }

  public static X509Certificate sign(String CN, PublicKey publicKey, Instant expiryDate, long serialNumber, X509Certificate caCert, PrivateKey privateKey) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    return sign(CN, publicKey, Instant.now(), expiryDate, serialNumber, caCert, privateKey);
  }

  public static X509Certificate sign(String CN, PublicKey publicKey, Instant startDate, Instant expiryDate, long serialNumber, X509Certificate caCert, PrivateKey privateKey) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    return sign(CN, publicKey, startDate, expiryDate, BigInteger.valueOf(serialNumber), caCert, privateKey);
  }

  /** @return a random positive serial number of 128 bits, as CAs use so that serials cannot be predicted */
  static BigInteger randomSerial() {
    BigInteger serial;
    do {
      serial = new BigInteger(SERIAL_BITS, random);
    } while (serial.signum() == 0);
    return serial;
  }

  /**
   * Issues a TLS end-entity certificate: it is no CA (basicConstraints CA:false, critical), may be used to
   * sign and to encipher keys (keyUsage digitalSignature, keyEncipherment), and as a TLS server and client
   * (extendedKeyUsage serverAuth, clientAuth).
   */
  public static X509Certificate sign(String CN, PublicKey publicKey, Instant startDate, Instant expiryDate, BigInteger serialNumber, X509Certificate caCert, PrivateKey privateKey) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    X500Principal subjectName = new X500Principal("CN="+CN);

    X509v3CertificateBuilder certGen = new JcaX509v3CertificateBuilder(
        caCert.getSubjectX500Principal(), serialNumber, Date.from(startDate), Date.from(expiryDate),
        subjectName, publicKey);

    try {
      JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
      certGen.addExtension(Extension.authorityKeyIdentifier, false,
                              extUtils.createAuthorityKeyIdentifier(caCert));
      certGen.addExtension(Extension.subjectKeyIdentifier, false,
                              extUtils.createSubjectKeyIdentifier(publicKey));
      certGen.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
      certGen.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
      certGen.addExtension(Extension.extendedKeyUsage, false,
                              new ExtendedKeyUsage(new KeyPurposeId[] {KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth}));
    } catch (CertIOException cioe) {
      throw new CertificateEncodingException(cioe);
    }

    return generate(certGen.build(signer(DEFAULT_SIGNATURE_ALGORITHM, privateKey)));   // note: private key of CA
  }

  public X509Certificate sign(String CN, PublicKey publicKey, Instant expiryDate, long serialNumber) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    return recordIssuance(CN, sign(CN,publicKey,expiryDate,serialNumber,cert,keyPair.getPrivate()));
  }


  /** Environment variable that holds the password of the CA store (see {@link #main}). */
  public static final String CA_PASSWORD_ENV = "FREEPASTRY_CA_PASSWORD";
  /** Environment variable that holds the password of a node store written with {@code -store}. */
  public static final String NODE_PASSWORD_ENV = "FREEPASTRY_NODE_PASSWORD";

  private static void requirePassword(char[] chars, String what) {
    if (chars == null || chars.length == 0) throw new IllegalArgumentException("The "+what+" key store needs a non-empty password");
  }

  /**
   * Opens a file for writing that only its owner can read: a new file is created with mode 0600 (POSIX) so it is
   * never readable by others, not even briefly; an existing one is tightened to 0600 first. Where the file system
   * has no POSIX permissions (e.g. Windows) the file is written with the platform defaults.
   */
  public static OutputStream newPrivateOutputStream(Path file) throws IOException {
    if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
      if (Files.exists(file)) {
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
      } else {
        Files.createFile(file, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      }
    }
    return Files.newOutputStream(file);
  }

  /**
   * Reads a password without ever taking it from the command line (visible in {@code ps}): from the environment
   * variable, else from the console without echo.
   *
   * @param envVar name of the environment variable
   * @param prompt shown on the console
   * @throws IllegalStateException if neither is available, or the password is empty
   */
  public static char[] readPassword(String envVar, String prompt) {
    return readPassword(envVar, prompt, System.getenv(), System.console());
  }

  public static char[] readPassword(String envVar, String prompt, Map<String, String> env, Console console) {
    String value = env.get(envVar);
    char[] chars;
    if (value != null) {
      chars = value.toCharArray();
    } else if (console != null) {
      chars = console.readPassword("%s", prompt);
    } else {
      throw new IllegalStateException("No password: set "+envVar+" or run on a terminal");
    }
    if (chars == null || chars.length == 0) throw new IllegalStateException("The password must not be empty ("+envVar+")");
    return chars;
  }

  /** also the aliases of a node store, which {@code CertifiedNodeIdFactory} reads */
  public static final String CA_STORE_PRIVATE = NodeIdCertificate.NODE_KEY_ALIAS;
  public static final String CA_STORE_CERT = NodeIdCertificate.CA_CERT_ALIAS;

  /** Operator flag that allows {@code -cn}: without it the CA chooses every node id. */
  public static final String ALLOW_CHOSEN_ID_FLAG = "-allow-chosen-id";
  /** Command that re-issues the CA certificate as version 3 for the same key and subject, see {@link #reissueCATool}. */
  public static final String REISSUE_CA_COMMAND = "-reissue-ca";

  /**
   *
   * -ca CAname [-cn newCN -allow-chosen-id] [-store] [-test] | -ca CAname -reissue-ca
   * <p>
   * The password of the CA store comes from the environment variable {@value #CA_PASSWORD_ENV}, else it is asked
   * on the console; it must not be empty. The old {@code -p password} option is refused: a password on the command
   * line is visible to every user of the machine in {@code ps}. With {@code -store} the node store and its key get
   * the password in {@value #NODE_PASSWORD_ENV} (or the console), and the file is readable by its owner only.
   * <p>
   * Without -cn the node gets a random node id the CA chooses ({@link #randomNodeId()}) as its common name, which
   * binds the id to the certificate (see {@link NodeIdCertificate}). -cn lets whoever asks for the certificate pick
   * the id, and is refused unless the operator adds {@value #ALLOW_CHOSEN_ID_FLAG} (tests, migrating existing ids);
   * it must be a node id (40 hex digits) to be usable with {@code CertifiedNodeIdFactory}. Every certificate is
   * recorded in {@code <CAname>}{@value #ISSUANCE_LOG_SUFFIX}. {@value #REISSUE_CA_COMMAND} replaces the CA
   * certificate with a version 3 one for the same key and subject (see {@link #reissueCATool}) and issues nothing.
   */
  public static void main(String[] args) throws Exception {
    String caName = "MyCA";
    String CN = null;
    boolean test = false;
    boolean store = false;
    boolean allowChosenId = false;
    boolean reissueCA = false;
    for (int c = 0; c < args.length; c++) {
      if (args[c].equalsIgnoreCase("-p")) {
        throw new IllegalArgumentException("-p is no longer supported: a password on the command line is visible in ps. Set "+CA_PASSWORD_ENV+" or enter it on the console.");
      } else if (args[c].equalsIgnoreCase("-ca")) {
        caName = args[c+1];
      } else if (args[c].equalsIgnoreCase("-cn")) {
        CN = args[c+1];
      } else if (args[c].equalsIgnoreCase("-test")) {
        test = true;
      } else if (args[c].equalsIgnoreCase("-store")) {
        store = true;
      } else if (args[c].equalsIgnoreCase(ALLOW_CHOSEN_ID_FLAG)) {
        allowChosenId = true;
      } else if (args[c].equalsIgnoreCase(REISSUE_CA_COMMAND)) {
        reissueCA = true;
      }
    }
    if (CN != null && !allowChosenId) {
      throw new IllegalArgumentException("-cn lets the requester choose the node id (and place itself next to a target key); the CA chooses ids unless the operator adds "+ALLOW_CHOSEN_ID_FLAG);
    }

    char[] chars = readPassword(CA_PASSWORD_ENV, "Password of the CA store "+caName+"-store: ");
    if (reissueCA) {
      CertificateAuthorityTool reissued = reissueCATool(caName, chars);
      Arrays.fill(chars, '\0');
      System.out.println(reissued.getCertificate());
      System.out.println("Re-issued the CA certificate in "+caName+"-store; put it into the node stores (alias "+NodeIdCertificate.CA_CERT_ALIAS+")");
      return;
    }
    char[] nodePw = store ? readPassword(NODE_PASSWORD_ENV, "Password of the new node store: ") : null;

    if (CN == null) {
      CN = randomNodeId().toStringFull();
    }

    CertificateAuthorityTool caTool = getCATool(caName,chars);

    KeyPair pair = generateKeyPair();
    X509Certificate cert = caTool.sign(CN,pair.getPublic());

    if (store) {
      // initialize the clientStore
      KeyStore clientStore = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
      clientStore.load(null, null);

      // store the private key under the client cert
      clientStore.setKeyEntry(NodeIdCertificate.NODE_KEY_ALIAS,pair.getPrivate(), nodePw, new Certificate[] {cert});

      // store the CA cert
      clientStore.setCertificateEntry(NodeIdCertificate.CA_CERT_ALIAS, caTool.getCertificate());

      // write the clientStore to disk
      String fileName = CN+".store";
      try (OutputStream fos = newPrivateOutputStream(Path.of(fileName))) {
        clientStore.store(fos, nodePw);
      }
      System.out.println("Stored to "+fileName);
    }

    if (test) {
      // write out the certs

      System.out.println("Cert Type:"+cert.getType()+" len:"+cert.getEncoded().length);

      // serialize/deserialize
      X509Serializer serializer = new X509Serializer();
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      serializer.serialize(caTool.getCertificate(), sob);

      SimpleInputBuffer sib = new SimpleInputBuffer(sob.getBytes());
      X509Certificate caCert = serializer.deserialize(sib);

      cert.verify(caCert.getPublicKey());
      System.out.println("cert verified.");

      System.out.println(caCert);
      System.out.println(cert);

      System.out.println(pair.getPublic().getFormat()+" "+pair.getPublic().getAlgorithm());
      System.out.println(pair.getPrivate().getFormat()+" "+pair.getPrivate().getAlgorithm());

      Signature signer = Signature.getInstance(DEFAULT_SIGNATURE_ALGORITHM, SecurityUtils.BOUNCY_CASTLE);
      signer.initSign(pair.getPrivate());

      byte[] msg = new byte[400];
      random.nextBytes(msg);

      signer.update(msg);
      byte[] signature = signer.sign();

      System.out.println(signer);

      System.out.println("Signature length:"+signature.length);

      Signature verifier = Signature.getInstance(DEFAULT_SIGNATURE_ALGORITHM, SecurityUtils.BOUNCY_CASTLE);
      verifier.initVerify(cert);
      verifier.update(msg);
      System.out.println("verified:"+verifier.verify(signature));

      KeyPair bogusPair = generateKeyPair();

      try {
        cert.verify(bogusPair.getPublic());
        System.out.println("WARNING!  Bogus key verified!!!");
      } catch (InvalidKeyException | SignatureException invalidKeyException) {
        System.out.println("bogus didn't verify.");
      }

      Signature bogusVerifier = Signature.getInstance(DEFAULT_SIGNATURE_ALGORITHM, SecurityUtils.BOUNCY_CASTLE);
      bogusVerifier.initVerify(bogusPair.getPublic());
      bogusVerifier.update(msg);
      System.out.println("bogus verify: "+bogusVerifier.verify(signature));

      verifier.update(msg);
      System.out.println("verified 2:"+verifier.verify(signature));

      msg[0]++;
      verifier.update(msg);
      System.out.println("verified (should fail):"+verifier.verify(signature));
    }
    Arrays.fill(chars, '\0');
    if (nodePw != null) Arrays.fill(nodePw, '\0');
  }
}
