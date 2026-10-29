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

import java.lang.ref.SoftReference;
import java.util.HashMap;

/**
 * Class which implements a Soft-Reference based HashMap, allowing the garbage
 * collector to collection stuff if memory pressure is tight.  Should be transparent
 * to applications, except that items may disappear.
 *
 * @author Alan Mislove
 */
public class SoftHashMap<K, V> extends HashMap<K, V> {
  
  /**
   * Returns the soft reference stored in the underlying map under the given key.
   * The underlying map stores SoftReference objects in place of the values.
   *
   * @param key The key
   * @return The reference, or null
   */
  @SuppressWarnings("unchecked")
  private SoftReference<V> getReference(Object key) {
    return (SoftReference<V>) super.get(key);
  }
  
  /**
   * Stores a soft reference to the given value in the underlying map.
   *
   * @param key The key
   * @param value The value to reference
   */
  @SuppressWarnings("unchecked")
  private void putReference(K key, V value) {
    super.put(key, (V) new SoftReference<V>(value));
  }
  
  /**
   * Returns whether or not the key is contained in this map.  Only returns true if
   * the softreference has not been GC'ed.
   *
   * @param key The key to check for
   * @return The result
   */
  public boolean containsKey(Object key) {
    if (! super.containsKey(key))
      return false;
    
    if (super.get(key) == null) {
      return true;
    } else {
      return (getReference(key).get() != null);
    }
  }
  
  /**
   * Returns whether or not the value is contained in this map.  Only returns true if
   * the softreference has not been GC'ed.
   *
   * @param value The value to check for
   * @return The result
   */
  public boolean containsValue(Object value) {
    if (value == null) {
      return super.containsValue(null);
    } else {
      return super.containsValue(new SoftReference<Object>(value));
    }
  }
  
  /**
   * Returns the object associated with the key.  May return null, if the soft reference
   * has been GC'ed.
   * 
   * @param key The key
   * @return The value
   */
  public V get(Object key) {
    SoftReference<V> value = getReference(key);
    
    if (value == null) {
      return null;
    } else {
      V result = value.get();
      
      if (result != null) {
        return result;
      } else {
        remove(key);
        return null;
      }
    }
  }
  
  /**
   * Adds an entry to the soft hash map.  May not persist for very long, though.
   *
   * @param key The key
   * @param value The value
   * @return The previous value of the key
   */
  public V put(K key, V value) {
    V result = get(key);
    
    if (value != null) {
      putReference(key, value); 
    } else {
      super.put(key, null);
    }
    
    return result;
  }
}
