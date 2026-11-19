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
package rice.pastry;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import rice.p2p.util.HashUtils;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.standard.StandardAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Pins how Ids, IdSet fingerprints and application addresses are derived from SHA-256.
 */
public class IdDerivationTest {

  static byte[] sha256(String s) {
    return HashUtils.hash(s.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void idOfAStringIsItsSha256TruncatedTo160Bits() {
    rice.p2p.commonapi.Id id = new PastryIdFactory(null).buildId("abc");
    assertEquals(Id.build(Arrays.copyOf(sha256("abc"), Id.IdBitLength / 8)), id);
    assertEquals(Id.build(sha256("abc")), id, "Id.build only reads the first 160 bits");
  }

  @Test
  public void idSetFingerprintIsAFullSha256() {
    IdSet set = new IdSet();
    set.addId(Id.build(sha256("x")));
    byte[] bytes = new byte[Id.IdBitLength / 8];
    Id.build(sha256("x")).blit(bytes);
    assertEquals(32, set.hash().length);
    assertEquals(Arrays.toString(HashUtils.hash(bytes)), Arrays.toString(set.hash()));
  }

  @Test
  public void addressIsTwoBytesOfTheClassHashThenTwoOfTheInstanceHash() {
    byte[] classHash = sha256(IdDerivationTest.class.toString());
    byte[] instanceHash = sha256("instance");
    int expected = ((classHash[0] & 0xff) << 24) | ((classHash[1] & 0xff) << 16)
        | ((instanceHash[0] & 0xff) << 8) | (instanceHash[1] & 0xff);
    assertEquals(expected, StandardAddress.getAddress(IdDerivationTest.class, "instance", null));
    assertEquals(expected & 0xffff0000, StandardAddress.getAddress(IdDerivationTest.class, null, null));
  }

  @Test
  public void addressesOfClassesDifferInTheirSecondByte() {
    // the second byte used to be that of the hash of nothing, the same for every class
    int a = StandardAddress.getAddress(String.class, null, null);
    int b = StandardAddress.getAddress(Integer.class, null, null);
    byte[] stringHash = sha256(String.class.toString());
    byte[] integerHash = sha256(Integer.class.toString());
    assertNotEquals(stringHash[1], integerHash[1], "precondition");
    assertNotEquals((a >> 16) & 0xff, (b >> 16) & 0xff);
  }
}
