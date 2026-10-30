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
import java.util.Arrays;
import java.util.Objects;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.MathUtils;
import static org.mpisws.p2p.transport.peerreview.PeerReviewConstants.SEQS_PER_MILLI;

/**
 *
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
public class SnippetEntry {
  /** Sequence code: an absolute 8-byte sequence number follows (other codes are a millisecond delta). */
  private static final byte SEQ_CODE_ABSOLUTE = (byte)0xFF;
  /** Size code: an unsigned 2-byte length follows. */
  private static final byte SIZE_CODE_SHORT = (byte)0xFF;
  /** Size code: a 4-byte length follows (smaller codes are the length itself). */
  private static final byte SIZE_CODE_INT = (byte)0xFE;

  private final byte type;
  private final long seq;
  private boolean isHash = false;
  private final byte[] content; // may be a hash

  public static final int NUM_INDEXES = SEQS_PER_MILLI;

  public SnippetEntry(byte type, long seq, boolean isHash, byte[] content) {
    if (content.length == 0) throw new IllegalArgumentException("Content can't be zero-length");
    this.type = type;
    this.seq = seq;
    this.isHash = isHash;
    this.content = content;
  }

  public SnippetEntry(InputBuffer buf, int hashSize, SnippetEntry prev) throws IOException {
    this(buf, decodeSeq(buf, prev.seq), hashSize);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SnippetEntry that)) return false;
    if (this.type != that.type) return false;
    if (this.seq != that.seq) return false;
    if (this.isHash != that.isHash) return false;
    return Arrays.equals(this.content, that.content);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, seq, isHash, Arrays.hashCode(content));
  }

  public SnippetEntry(InputBuffer buf, long seq, int hashSize) throws IOException {
    type = buf.readByte();
    this.seq = seq;
    int size = decodeSize(buf);
    isHash = false;
    if (size == 0) {
      isHash = true;
      size = hashSize;
    }
    content = new byte[size];
    buf.read(content);
  }

  @Override
  public String toString() {
    return "SEntry type:"+type+" seq:"+seq+" "+(isHash?"hash":"not hash")+" len:"+content.length;
  }

  public void serialize(OutputBuffer buf, SnippetEntry prev) throws IOException {
    if (prev != null) {
      encodeSeq(buf, prev.seq);
    }
    buf.writeByte(type);
    encodeSize(buf);
    buf.write(content, 0, content.length);
  }

  protected static long decodeSeq(InputBuffer buf, long prevSeq) throws IOException {
    byte b = buf.readByte();
    if (b == (byte)0) return prevSeq+1;
    if (b == SEQ_CODE_ABSOLUTE) return buf.readLong();
    long howMuchBigger = MathUtils.uByteToInt(b)*NUM_INDEXES;
    long prevSeqMS = ((prevSeq/NUM_INDEXES)*NUM_INDEXES);
    return prevSeqMS+howMuchBigger;
  }

  protected void encodeSeq(OutputBuffer buf, long prevSeq) throws IOException {
    if (seq == prevSeq+1) {
      buf.writeByte((byte)0);
      return;
    }

    // If I end with SEQS_PER_MILLI
    if ((seq%NUM_INDEXES == 0)) {
      long foo = ((seq/NUM_INDEXES) - (prevSeq/NUM_INDEXES));
      if (foo < MathUtils.uByteToInt(SEQ_CODE_ABSOLUTE)) {
        if (foo < 0) throw new IOException("bug in encodeSeq.  foo:"+foo+" seq:"+seq+" prevSeq:"+prevSeq);
        buf.writeByte((byte)foo);
        return;
      }
    }

    buf.writeByte(SEQ_CODE_ABSOLUTE);
    buf.writeLong(seq);
  }

  public void encodeSize(OutputBuffer buf) throws IOException {
    if (isHash) {
      buf.writeByte((byte)0);
      return;
    }
    if (content.length < MathUtils.uByteToInt(SIZE_CODE_INT)) {
      buf.writeByte((byte)content.length);
      return;
    }
    if (content.length < 65535) {
      buf.writeByte(SIZE_CODE_SHORT);
      buf.writeShort((short)content.length);
      return;
    }
    buf.writeByte(SIZE_CODE_INT);
    buf.writeInt(content.length);
  }

  public int decodeSize(InputBuffer buf) throws IOException {
    byte b = buf.readByte();
    if (b == SIZE_CODE_SHORT) {
      return MathUtils.uShortToInt(buf.readShort());
    }
    if (b == SIZE_CODE_INT) {
      return buf.readInt();
    }
    return MathUtils.uByteToInt(b);
  }

  public byte[] getContent() {
    return content;
  }

  public boolean isHash() {
    return isHash;
  }

  public long getSeq() {
    return seq;
  }

  public byte getType() {
    return type;
  }
}
