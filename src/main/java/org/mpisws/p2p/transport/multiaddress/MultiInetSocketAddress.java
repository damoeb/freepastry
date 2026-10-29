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
import java.io.Serializable;
import java.net.InetAddress;
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
public class MultiInetSocketAddress implements Serializable {
  static InetSocketAddressSerializer serializer = new InetSocketAddressSerializer();
  
  // the address list, most external first
  protected InetSocketAddress address[];
  
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
  public int hashCode() {
    int result = 31173;
    for (int i = 0; i < address.length; i++) {
      result ^=  address[i].hashCode();
    }
    return result;
  }
  
  /**
   * Checks equality on address lists
   *
   * @param o The address list to compare to
   * @return The equality
   */
  public boolean equals(Object o) {
    if (o == null) return false;
    if (! (o instanceof MultiInetSocketAddress)) return false;
    MultiInetSocketAddress that = (MultiInetSocketAddress)o;
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
  public String toString() {
    String s = "";
    for (int ctr = 0; ctr < address.length; ctr++) {
      s+=address[ctr];
      if (ctr < address.length-1) s+=":";  
    }
    return s;
  }
  
  public void toStringShort(StringBuffer result) {
    for (int ctr = 0; ctr < address.length; ctr++) {
      InetSocketAddress theAddr = address[ctr];
      InetAddress theAddr2 = theAddr.getAddress();
      if (theAddr2 == null) {
        result.append(theAddr.toString());
      } else {
        String ha = theAddr2.getHostAddress();
        result.append(ha + ":" + theAddr.getPort());
      }
      if (ctr < address.length - 1) result.append(";");
    } // ctr 
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
   *  
   * @param buf
   * @return
   * @throws IOException
   */
  public static MultiInetSocketAddress build(InputBuffer buf) throws IOException {
    byte numAddresses = buf.readByte();
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
    for (int ctr = 0; ctr < address.length; ctr++) {
      serializer.serialize(address[ctr], buf);
    }
  }

  public short getSerializedLength() {
    int ret = 1; // num addresses
    for (int ctr = 0; ctr < address.length; ctr++) {
      ret += serializer.getSerializedLength(address[ctr]);
    }
    return (short)ret; 
  }

}


