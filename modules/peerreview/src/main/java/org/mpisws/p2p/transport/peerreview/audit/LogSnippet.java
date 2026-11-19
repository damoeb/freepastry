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
package org.mpisws.p2p.transport.peerreview.audit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.slf4j.Logger;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.rawserialization.WireLengths;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
  long long firstSeq
  byte extInfoLen
  [extInfo follows]
  hash baseHash
  --entry begin--           // 1 or more of these entries follow
  char type
  char sizeCode             // 0=hashed, 1-FD=size, FE=32-bit size follows, FF=16-bit size follows
 {short/int size}
  char content[]
  char nextSeqCode          // 0=+1, 1=(idx=0,us+=1), 2=(idx=0,us+=2), ..., FF=full seq  [does not exist for the last one]
 {long long seq}
  --entry end--
 * @author Jeff Hoye
 *
 */
public class LogSnippet {
  byte[] baseHash;
  private final List<SnippetEntry> entries;

  public LogSnippet(byte[] baseHash, List<SnippetEntry> entries) {
    this.baseHash = baseHash;
    this.entries = entries;
  }

  @Override
  public String toString() {
    return "LogSnippet["+entries.get(0).getSeq()+"-"+entries.get(entries.size()-1).getSeq()+"]";
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof LogSnippet that)) return false;
    if (!Arrays.equals(this.baseHash, that.baseHash)) return false;
    if (this.entries.size() != that.entries.size()) return false;
    Iterator<SnippetEntry> iterator = that.entries.iterator();
    for (SnippetEntry snippetEntry : this.entries) {
      if (!snippetEntry.equals(iterator.next())) return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(baseHash), entries);
  }

  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeLong(entries.get(0).getSeq());
    buf.writeByte((byte)0);
    buf.write(baseHash, 0, baseHash.length);
    Iterator<SnippetEntry> iterator = entries.iterator();
    SnippetEntry prev = iterator.next();
    prev.serialize(buf, null);
    while(iterator.hasNext()) {
      SnippetEntry cur = iterator.next();
      cur.serialize(buf, prev);
      prev = cur;
    }
  }

  public LogSnippet(InputBuffer buf, int hashSize) throws IOException {
    long firstSeq = buf.readLong();
    if (buf.readByte() != 0) throw new IOException("Unexpected extInfo");
    baseHash = WireLengths.readBytes(buf, hashSize);
    entries = new ArrayList<SnippetEntry>();
    SnippetEntry prev = new SnippetEntry(buf,firstSeq,hashSize);
    entries.add(prev);
    while(buf.bytesRemaining() == -2 || buf.bytesRemaining() > 0) {
      prev = new SnippetEntry(buf,hashSize,prev);
      entries.add(prev);
    }
  }

  public byte[] getBaseHash() {
    return baseHash;
  }

  public long getFirstSeq() {
    return entries.get(0).getSeq();
  }

  public Object getExtInfo() {
    return null;
  }

  public boolean checkHashChainContains(byte[] keyNodeHash, long keyNodeSeq, HashProvider transport, Logger logger) {
    assert(keyNodeHash != null && (keyNodeSeq >= 0));

    byte[] currentNodeHash = baseHash;

    logger.debug("Checking whether hash chain in snippet contains node #{}", keyNodeSeq);

    for (SnippetEntry entry : entries) {
      if (entry.getSeq() > keyNodeSeq) break; // fail

      logger.trace("Entry {}", entry);

      byte[] contentHash;
      if (entry.isHash()) {
        contentHash = entry.getContent();
      } else {
        contentHash = transport.hash(ByteBuffer.wrap(entry.getContent()));
      }

      currentNodeHash = transport.hash(entry.getSeq(), entry.getType(), currentNodeHash, contentHash);

      /* If this is the first entry, its hash must match the first authenticator in the challenge */

      if (entry.getSeq() == keyNodeSeq) {
        if (Arrays.equals(currentNodeHash, keyNodeHash)) {
          logger.trace("Yes, the node was found and has the specified hash");
          return true;
        } else {
          logger.trace("No, the node was found but has a different hash");
          return false;
        }
      }

      /* Skip ahead to the next entry in the snippet */

    }

    logger.trace("No, a node with this sequence number was not found");
    return false;
  }

  public List<SnippetEntry> getEntries() {
    return entries;
  }
}
