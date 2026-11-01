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

package org.mpisws.p2p.transport.multiaddress;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.mpisws.p2p.transport.simpleidentity.InetSocketAddressSerializer;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * The ordered list of addresses of a single node: the outermost (public) address first,
 * the innermost (bind) address last.
 *
 * @author Jeff Hoye
 */
public class MultiInetSocketAddress {
  static final InetSocketAddressSerializer serializer = new InetSocketAddressSerializer();

  /**
   * The most addresses {@link #build(InputBuffer)} accepts, for the callers that have no configuration at hand.
   */
  public static final int HARD_MAX_NUM_ADDRESSES = 8;

  // the address list, most external first
  private final InetSocketAddress address[];

  /**
   * Constructor for a node with a single address
   *
   * @param address The remote address
   */
  public MultiInetSocketAddress(InetSocketAddress address) {
    this(new InetSocketAddress[]{address});
  }

  public MultiInetSocketAddress(InetSocketAddress[] addressList) {
    this.address = addressList;
  }

  public MultiInetSocketAddress(InetSocketAddress outer,
      InetSocketAddress inner) {
    this(new InetSocketAddress[]{outer, inner});
  }

  /**
   * Returns the hashCode of this address list
   *
   * @return The hashCode
   */
  @Override
  public int hashCode() {
    int result = 31173;
    for (InetSocketAddress inetSocketAddress : address) {
      result ^=  inetSocketAddress.hashCode();
    }
    return result;
  }

  /**
   * Checks equality on address lists
   *
   * @param o The address list to compare to
   * @return The equality
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) return false;
    if (! (o instanceof MultiInetSocketAddress that)) return false;
    return addressEquals(that);
  }

  public boolean addressEquals(MultiInetSocketAddress that) {
    if (this.address.length != that.address.length) {
      // this code is here so we can say we are the same if one node knows they are firewalled and the other doesn't

      if (that.getInnermostAddress().equals(this.getInnermostAddress())) {
        return true;
      }
      return false;
    }
    for (int ctr = 0; ctr < this.address.length; ctr++) {
      if (!this.address[ctr].equals(that.address[ctr])) return false;
    }
    return true;
  }

  /**
   * Returns the addresses separated by colons
   *
   * @return The string
   */
  @Override
  public String toString() {
    String s = "";
    for (int ctr = 0; ctr < address.length; ctr++) {
      s+=address[ctr];
      if (ctr < address.length-1) s+=":";
    }
    return s;
  }

  public InetSocketAddress getInnermostAddress() {
    return address[address.length-1];
  }

  public InetSocketAddress getOutermostAddress() {
    return address[0];
  }

  public int getNumAddresses() {
    return address.length;
  }

  public InetSocketAddress getAddress(int index) {
    return address[index];
  }

  /**
   *   MultiInetSocketAddress: (IPV4):
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   numAddrs    +  IPVersion 0  +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   internet address 0          ...
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   port 0                      +  IPVersion 1  +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   internet address 1          ...
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   port 1                      +  IPVersion k  +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   internet address k          ...
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   port k                      +       ...
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   */
  public static MultiInetSocketAddress build(InputBuffer buf) throws IOException {
    return build(buf, HARD_MAX_NUM_ADDRESSES);
  }

  /**
   * Like {@link #build(InputBuffer)}, but rejects an address list longer than {@code maxNumAddresses}.
   *
   * @throws IOException if the number of addresses is not between 1 and {@code maxNumAddresses}
   */
  public static MultiInetSocketAddress build(InputBuffer buf, int maxNumAddresses) throws IOException {
    byte numAddresses = buf.readByte();
    if (numAddresses < 1 || numAddresses > maxNumAddresses) {
      throw new IOException("Invalid number of addresses: " + numAddresses + " (max: " + maxNumAddresses + ")");
    }
    InetSocketAddress[] saddr = new InetSocketAddress[numAddresses];
    for (int ctr = 0; ctr < numAddresses; ctr++) {
      saddr[ctr] = serializer.deserialize(buf, null, null);
    }
    return new MultiInetSocketAddress(saddr);
  }

  /**
   * Writes the format described in {@link #build(InputBuffer)}.
   *
   * @param buf the buffer to write to
   */
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)address.length);
    for (InetSocketAddress inetSocketAddress : address) {
      serializer.serialize(inetSocketAddress, buf);
    }
  }

  public short getSerializedLength() {
    int ret = 1; // num addresses
    for (InetSocketAddress inetSocketAddress : address) {
      ret += serializer.getSerializedLength(inetSocketAddress);
    }
    return (short)ret;
  }

}
