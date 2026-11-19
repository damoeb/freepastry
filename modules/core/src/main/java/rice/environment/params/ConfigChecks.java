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
package rice.environment.params;

/**
 * Range checks for the typed config records that components build from {@link Parameters}. Each check
 * names the parameter key, so a bad value in a {@code .params} file is easy to find.
 */
public final class ConfigChecks {
  private ConfigChecks() {}

  /**
   * @return value, if it is greater than zero
   * @throws IllegalArgumentException naming key otherwise
   */
  public static int requirePositive(String key, int value) {
    if (value <= 0) {
      throw new IllegalArgumentException(key + " must be > 0, but is " + value);
    }
    return value;
  }

  /**
   * @return value, if it is greater than zero
   * @throws IllegalArgumentException naming key otherwise
   */
  public static long requirePositive(String key, long value) {
    if (value <= 0) {
      throw new IllegalArgumentException(key + " must be > 0, but is " + value);
    }
    return value;
  }

  /**
   * @return value, if it is zero or greater
   * @throws IllegalArgumentException naming key otherwise
   */
  public static int requireNonNegative(String key, int value) {
    if (value < 0) {
      throw new IllegalArgumentException(key + " must be >= 0, but is " + value);
    }
    return value;
  }

  /**
   * @return value, if it is zero or greater
   * @throws IllegalArgumentException naming key otherwise
   */
  public static long requireNonNegative(String key, long value) {
    if (value < 0) {
      throw new IllegalArgumentException(key + " must be >= 0, but is " + value);
    }
    return value;
  }

  /**
   * @return value, if it lies within [min, max]
   * @throws IllegalArgumentException naming key otherwise
   */
  public static double inRange(String key, double value, double min, double max) {
    if (!(value >= min && value <= max)) {
      throw new IllegalArgumentException(key + " must be within [" + min + ", " + max + "], but is " + value);
    }
    return value;
  }
}
