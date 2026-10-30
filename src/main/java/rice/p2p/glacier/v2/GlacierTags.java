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
package rice.p2p.glacier.v2;

/**
 * The tags Glacier puts on its messages, which tell the statistics what each message was sent for.
 */
final class GlacierTags {
  static final char NEIGHBOR = 1;
  static final char SYNC = 2;
  static final char SYNC_MANIFESTS = 3;
  static final char SYNC_FETCH = 4;
  static final char HANDOFF = 5;
  static final char DEBUG = 6;
  static final char REFRESH = 7;
  static final char INSERT = 8;
  static final char LOOKUP_HANDLES = 9;
  static final char LOOKUP = 10;
  static final char FETCH = 11;
  static final char LOCAL_SCAN = 12;
  static final char MAX = 13;

  private GlacierTags() {
  }
}
