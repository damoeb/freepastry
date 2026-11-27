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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HashUtilsTest {

  /** SHA-256("abc"), FIPS 180-2 appendix B.1 */
  static final byte[] SHA256_ABC = HexFormat.of().parseHex("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  static final byte[] ABC = "abc".getBytes(StandardCharsets.US_ASCII);

  @Test
  public void contentHashIsSha256() {
    assertEquals(32, HashUtils.LENGTH);
    assertArrayEquals(SHA256_ABC, HashUtils.hash(ABC));
  }

  @Test
  public void contentHashOfPartsIsThatOfTheirConcatenation() {
    assertArrayEquals(SHA256_ABC, HashUtils.hash("a".getBytes(StandardCharsets.US_ASCII), "bc".getBytes(StandardCharsets.US_ASCII)));
    assertArrayEquals(SHA256_ABC, HashUtils.hash("xabcx".getBytes(StandardCharsets.US_ASCII), 1, 3));
    assertArrayEquals(SHA256_ABC, HashUtils.newDigest().digest(ABC));
  }

  @Test
  public void idMaterialIsSha256TruncatedTo160Bits() {
    assertEquals(rice.pastry.Id.IdBitLength / 8, HashUtils.ID_LENGTH);
    assertArrayEquals(Arrays.copyOf(SHA256_ABC, 20), HashUtils.hashToIdMaterial(ABC));
  }
}
