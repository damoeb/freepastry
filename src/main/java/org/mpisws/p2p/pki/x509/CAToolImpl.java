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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.security.spec.RSAKeyGenParameterSpec;
import java.util.Calendar;
import java.util.Date;
import javax.security.auth.x500.X500Principal;

import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509v1CertificateBuilder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v1CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.p2p.util.tuples.Tuple;
import rice.pastry.Id;
import rice.pastry.standard.RandomNodeIdFactory;

public class CAToolImpl implements CATool {

  static {
    // install BC
    Security.addProvider(new BouncyCastleProvider());
  }
  
  static SecureRandom random = new SecureRandom();
  /** Was SHA1withRSA, which JDK 25 rejects for certificate paths in TLS. */
  public static final String DEFAULT_SIGNATURE_ALGORITHM = "SHA256withRSA";
  /** RSA modulus size for generated CA and node keys (was 768, which JDK 25 rejects in TLS). */
  public static final int DEFAULT_KEY_SIZE = 2048;
  /** Type of the key stores this tool reads and writes (was BouncyCastle's legacy "UBER"). */
  public static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

  X509Certificate cert; 
  KeyPair keyPair;
  
  public CAToolImpl(X509Certificate cert, KeyPair caPair) {
    this.cert = cert;
    this.keyPair = caPair;
  }
  
  public X509Certificate getCertificate() {
    return cert;
  }
  
  /**
   * 
   * @param CN only used when generating a new CA
   * @param pw
   * @return
   * @throws KeyStoreException
   * @throws NoSuchProviderException
   * @throws NoSuchAlgorithmException
   * @throws CertificateException
   * @throws FileNotFoundException
   * @throws IOException
   * @throws UnrecoverableKeyException
   * @throws InvalidKeyException
   * @throws IllegalStateException
   * @throws SignatureException
   * @throws InvalidAlgorithmParameterException
   */
  public static CAToolImpl getCATool(String CN, char[] pw) throws KeyStoreException, NoSuchProviderException, NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException, InvalidKeyException, IllegalStateException, SignatureException, InvalidAlgorithmParameterException {

    X509Certificate caCert;
    KeyPair caPair;
    
    File caStoreFile = new File(CN+"-store");
    if (caStoreFile.exists()) {
      KeyStore store = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
      try (FileInputStream fis = new FileInputStream(caStoreFile)) {
        store.load(fis, pw);
      } catch (IOException ioe) {
        throw new IOException("Could not load "+caStoreFile+" as a "+DEFAULT_KEYSTORE_TYPE+" key store: wrong password, or a store written by an older FreePastry (UBER format, no longer supported)", ioe);
      }
      caCert = (X509Certificate)store.getCertificate(CA_STORE_CERT);
      PrivateKey priv = (PrivateKey)store.getKey(CA_STORE_PRIVATE, pw);
      caPair = new KeyPair(caCert.getPublicKey(),priv);
    } else {
      // make a self-signed cert
      Date exp = new Date();
      Calendar cal = Calendar.getInstance();
      cal.setTime(exp);
      cal.add(Calendar.YEAR, 10);
      exp = cal.getTime();
      Tuple<X509Certificate, KeyPair> t = generateNewCA(CN, exp);
      caCert = t.a();
      caPair = t.b();
      
      Certificate[] chain = new Certificate[1];
      chain[0] = caCert;
      
      System.out.println(caCert);
      
      // store it
      // PKCS12 cannot hold a bare public key: it is read back from the certificate
      KeyStore store = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
      store.load(null, null);
      store.setKeyEntry(CA_STORE_PRIVATE, caPair.getPrivate(), pw, chain);
      store.setCertificateEntry(CA_STORE_CERT, caCert);

      try (FileOutputStream fos = new FileOutputStream(caStoreFile)) {
        store.store(fos, pw);
      }
    }
    return new CAToolImpl(caCert, caPair);
  }
  
  public static Tuple<X509Certificate, KeyPair> generateNewCA(String CN, Date expiryDate) throws CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException, InvalidAlgorithmParameterException {
    KeyPair caPair = generateKeyPair();

    X509Certificate cert = generateNewCA(CN,new Date(), expiryDate, 1, caPair, DEFAULT_SIGNATURE_ALGORITHM);
    
    return new Tuple<X509Certificate, KeyPair>(cert,caPair);
  }

  /**
   * @return a new RSA key pair of {@link #DEFAULT_KEY_SIZE} bits, suitable for a CA or a node certificate
   */
  public static KeyPair generateKeyPair() throws NoSuchAlgorithmException, NoSuchProviderException, InvalidAlgorithmParameterException {
    KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA", "BC");
    keyPairGen.initialize(new RSAKeyGenParameterSpec(DEFAULT_KEY_SIZE, RSAKeyGenParameterSpec.F4), random);
    return keyPairGen.generateKeyPair();
  }
  
  /**
   * 
   * @param CN common name
   * @param startDate 
   * @param expiryDate
   * @param serialNumber
   * @param keyPair
   * @return the CA cert
   * @throws SignatureException 
   * @throws NoSuchAlgorithmException 
   * @throws NoSuchProviderException 
   * @throws IllegalStateException 
   * @throws InvalidKeyException 
   * @throws CertificateEncodingException 
   */
  public static X509Certificate generateNewCA(String CN, Date startDate, Date expiryDate, long serialNumber, KeyPair keyPair, String signatureAlgorithm) throws CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    X500Principal dnName = new X500Principal("CN="+CN);

    X509v1CertificateBuilder certGen = new JcaX509v1CertificateBuilder(
        dnName, BigInteger.valueOf(serialNumber), startDate, expiryDate,
        dnName,                                         // note: same as issuer
        keyPair.getPublic());

    return generate(certGen.build(signer(signatureAlgorithm, keyPair.getPrivate())));
  }  
  
  /**
   * @param signatureAlgorithm e.g. DEFAULT_SIGNATURE_ALGORITHM
   * @param privateKey the key the certificate is signed with
   */
  private static ContentSigner signer(String signatureAlgorithm, PrivateKey privateKey) throws InvalidKeyException {
    try {
      return new JcaContentSignerBuilder(signatureAlgorithm).setProvider("BC").build(privateKey);
    } catch (OperatorCreationException oce) {
      throw new InvalidKeyException("Could not create signer for "+signatureAlgorithm, oce);
    }
  }
  
  private static X509Certificate generate(org.bouncycastle.cert.X509CertificateHolder holder) throws CertificateEncodingException {
    try {
      return new JcaX509CertificateConverter().setProvider("BC").getCertificate(holder);
    } catch (CertificateException ce) {
      throw new CertificateEncodingException(ce);
    }
  }
  
  public X509Certificate sign(String CN, PublicKey key) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    Date exp = new Date();
    Calendar cal = Calendar.getInstance();
    cal.setTime(exp);
    cal.add(Calendar.YEAR, 1);
    exp = cal.getTime();
    return sign(CN,key,exp,System.currentTimeMillis());
  }
  
  public static X509Certificate sign(String CN, PublicKey publicKey, Date expiryDate, long serialNumber, X509Certificate caCert, PrivateKey privateKey) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    X500Principal subjectName = new X500Principal("CN="+CN);

    X509v3CertificateBuilder certGen = new JcaX509v3CertificateBuilder(
        caCert.getSubjectX500Principal(), BigInteger.valueOf(serialNumber), new Date(), expiryDate,
        subjectName, publicKey);

    try {
      JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
      certGen.addExtension(Extension.authorityKeyIdentifier, false,
                              extUtils.createAuthorityKeyIdentifier(caCert));
      certGen.addExtension(Extension.subjectKeyIdentifier, false,
                              extUtils.createSubjectKeyIdentifier(publicKey));
    } catch (CertIOException cioe) {
      throw new CertificateEncodingException(cioe);
    }

    return generate(certGen.build(signer(DEFAULT_SIGNATURE_ALGORITHM, privateKey)));   // note: private key of CA
  }
  
  public X509Certificate sign(String CN, PublicKey publicKey, Date expiryDate, long serialNumber) throws CertificateParsingException, CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
    return sign(CN,publicKey,expiryDate,serialNumber,cert,keyPair.getPrivate());
  }
  
  public static final String CA_STORE_PRIVATE = "private";
  /** No longer written: PKCS12 stores cannot hold a bare public key, it is taken from {@link #CA_STORE_CERT}. */
  public static final String CA_STORE_PUBLIC = "public";
  public static final String CA_STORE_CERT = "cert";
  
  /**
   * 
   * -p CApassword -ca CAname -cn newCN
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    char[] pw = new char[0];
    String caName = "MyCA";
    String CN = null;
    boolean test = false;
    boolean store = false;
    for (int c = 0; c < args.length; c++) {
      if (args[c].equalsIgnoreCase("-p")) {
        pw = args[c+1].toCharArray();
      } else if (args[c].equalsIgnoreCase("-ca")) {
        caName = args[c+1];
      } else if (args[c].equalsIgnoreCase("-cn")) {
        CN = args[c+1];
      } else if (args[c].equalsIgnoreCase("-test")) {
        test = true;
      } else if (args[c].equalsIgnoreCase("-store")) {
        store = true;
      }
    }
    
    Environment env = null;
    if (CN == null) {
      env = new Environment();
      Id id = new RandomNodeIdFactory(env).generateNodeId();
      CN = id.toStringFull();
    }
        
    CATool caTool = getCATool(caName,pw);
   
    KeyPair pair = generateKeyPair();
    X509Certificate cert = caTool.sign(CN,pair.getPublic());

    if (store) {
      // initialize the clientStore
      KeyStore clientStore = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
      clientStore.load(null, null);
      
      // store the private key under the client cert
      clientStore.setKeyEntry("private",pair.getPrivate(), "".toCharArray(), new Certificate[] {cert});
      
      // store the CA cert
      clientStore.setCertificateEntry("cert", caTool.getCertificate());
      
      // write the clientStore to disk
      String fileName = CN+".store";
      FileOutputStream fos = new FileOutputStream(fileName);
      // store the clientStore under a blank password, if you want a password on the client store, do it here
      clientStore.store(fos, "".toCharArray());
      fos.close();
      System.out.println("Stored to "+fileName);
    }

    if (test) {
      // write out the certs
      
      System.out.println("Cert Type:"+cert.getType()+" len:"+cert.getEncoded().length);
      
      // serialize/deserialize
      X509Serializer serializer = new X509SerializerImpl();
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
      
      Signature signer = Signature.getInstance(DEFAULT_SIGNATURE_ALGORITHM,"BC");
      signer.initSign(pair.getPrivate());
      
      byte[] msg = new byte[400];
      random.nextBytes(msg);
      
      signer.update(msg);
      byte[] signature = signer.sign();
      
      System.out.println(signer);
      
      System.out.println("Signature length:"+signature.length);
      
      Signature verifier = Signature.getInstance(DEFAULT_SIGNATURE_ALGORITHM, "BC");
      verifier.initVerify(cert);
      verifier.update(msg);
      System.out.println("verified:"+verifier.verify(signature));
  
      if (true) {
        KeyPair bogusPair = generateKeyPair();
    
        try {
          cert.verify(bogusPair.getPublic());
          System.out.println("WARNING!  Bogus key verified!!!");
        } catch (InvalidKeyException | SignatureException se) {
          System.out.println("bogus didn't verify.");
        }
     
        Signature bogusVerifier = Signature.getInstance(DEFAULT_SIGNATURE_ALGORITHM, "BC");
        bogusVerifier.initVerify(bogusPair.getPublic());
        bogusVerifier.update(msg);
        System.out.println("bogus verify: "+bogusVerifier.verify(signature));
      }
      
      verifier.update(msg);
      System.out.println("verified 2:"+verifier.verify(signature));
    
      msg[0]++;
      verifier.update(msg);
      System.out.println("verified (should fail):"+verifier.verify(signature));
    }
    if (env != null) env.destroy();
  }  
}
