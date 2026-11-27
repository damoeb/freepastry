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
package rice.p2p.util.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.Arrays;

import rice.p2p.util.SecurityUtils;

public class SecurityUtilsUnit {

  /**
   * Tests the security service.
   *
   * @param argv The command line arguments
   * @throws NoSuchAlgorithmException If the encryption does not happen
   *      properly
   * @throws IOException If the encryption does not happen properly
   * @throws ClassNotFoundException If the encryption does not happen
   *      properly
   */
  public static void main(String[] argv) throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
    System.out.println("SecurityUtils Test Suite");
    System.out.println("-------------------------------------------------------------");
    System.out.println("  Initializing Tests");
    System.out.print("    Generating key pairs\t\t\t\t");

    KeyPair pair = SecurityUtils.generateKeyAsymmetric();
    KeyPair pair2 = SecurityUtils.generateKeyAsymmetric();
    System.out.println("[ DONE ]");

    System.out.print("    Building cipher\t\t\t\t\t");

    System.out.println("[ DONE ]");
    System.out.println("-------------------------------------------------------------");
    System.out.println("  Running Tests");

    String testString = "test";
    byte[] testStringByte = testString.getBytes(StandardCharsets.UTF_8);

    System.out.print("    Testing symmetric encryption\t\t\t");

    byte[] key = SecurityUtils.generateKeySymmetric();
    byte[] testStringCipherText = SecurityUtils.encryptSymmetric(testStringByte, key);
    byte[] testStringPlainText = SecurityUtils.decryptSymmetric(testStringCipherText, key);

    if (Arrays.equals(testStringByte, testStringPlainText)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Length:\t" + testStringByte.length);
      System.out.println("    Cipher Len:\t" + testStringCipherText.length);
      System.out.println("    Output Len:\t" + testStringPlainText.length);
    }

    System.out.print("    Testing signing and verification (phase 1)\t\t");

    byte[] testStringSig = SecurityUtils.sign(testStringByte, pair.getPrivate());

    if (SecurityUtils.verify(testStringByte, testStringSig, pair.getPublic())) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Length:\t" + testStringByte.length);
      System.out.println("    Sig Len:\t" + testStringSig.length);
    }

    System.out.print("    Testing signing and verification (phase 2)\t\t");

    testStringSig[0]++;

    if (! SecurityUtils.verify(testStringByte, testStringSig, pair.getPublic())) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Length:\t" + testStringByte.length);
      System.out.println("    Sig Len:\t" + testStringSig.length);
    }

    System.out.print("    Testing asymmetric encryption\t\t\t");

    byte[] testStringEncrypted = SecurityUtils.encryptAsymmetric(testStringByte, pair.getPublic());
    byte[] testStringDecrypted = SecurityUtils.decryptAsymmetric(testStringEncrypted, pair.getPrivate());

    if (Arrays.equals(testStringByte, testStringDecrypted)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Length:\t" + testStringByte.length);
      System.out.println("    Enc Len:\t" + testStringEncrypted.length);
      System.out.println("    Dec Len:\t" + testStringDecrypted.length);
    }

    System.out.print("    Testing asymmetic symmetric key encryption\t\t");

    byte[] keySym = SecurityUtils.generateKeySymmetric();

    if (Arrays.equals(SecurityUtils.encryptAsymmetric(keySym, pair.getPublic()),
                      SecurityUtils.encryptAsymmetric(keySym, pair2.getPublic()))) {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + java.util.HexFormat.of().formatHex(keySym));
      System.out.println("    Output 1: \t" + java.util.HexFormat.of().formatHex(SecurityUtils.encryptAsymmetric(keySym, pair.getPublic())));
      System.out.println("    Output 2: \t" + java.util.HexFormat.of().formatHex(SecurityUtils.encryptAsymmetric(keySym, pair2.getPublic())));
    } else {
      System.out.println("[ PASSED ]");
    }

    System.out.print("    Testing asymmetic public writing\t\t\t");

    PublicKey publicKey = SecurityUtils.decodePublicKey(SecurityUtils.encodePublicKey(pair.getPublic()));

    if (! Arrays.equals(publicKey.getEncoded(), pair.getPublic().getEncoded())) {
      System.out.println("[ FAILED ]");
      System.out.println("  Output 1: \t" + java.util.HexFormat.of().formatHex(SecurityUtils.encryptAsymmetric(keySym, publicKey)));
      System.out.println("     Res 1: \t" + java.util.HexFormat.of().formatHex(SecurityUtils.decryptAsymmetric(SecurityUtils.encryptAsymmetric(keySym, publicKey), pair.getPrivate())));
      System.out.println("  Output 2: \t" + java.util.HexFormat.of().formatHex(SecurityUtils.encryptAsymmetric(keySym, pair.getPublic())));
      System.out.println("     Res 2: \t" + java.util.HexFormat.of().formatHex(SecurityUtils.decryptAsymmetric(SecurityUtils.encryptAsymmetric(keySym, pair.getPublic()), pair.getPrivate())));
    } else {
      System.out.println("[ PASSED ]");
    }

    System.out.print("    Testing asymmetic serialized writing\t\t");

    testStringEncrypted = SecurityUtils.encryptAsymmetric(testStringByte, publicKey);
    testStringDecrypted = SecurityUtils.decryptAsymmetric(testStringEncrypted, pair.getPrivate());

    if (Arrays.equals(testStringByte, testStringDecrypted)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Length:\t" + testStringByte.length);
      System.out.println("    Enc Len:\t" + testStringEncrypted.length);
      System.out.println("    Dec Len:\t" + testStringDecrypted.length);
    }

    System.out.println("-------------------------------------------------------------");
  }
}
