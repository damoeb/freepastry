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
package rice;

import java.util.concurrent.CompletionStage;

import org.jspecify.annotations.Nullable;

/**
 * The outcome of one asynchronous operation of a fan-out: its value, or the exception it failed with. Here a
 * failure is a result like any other; it does not fail the whole fan-out (see {@link Futures#collect}).
 *
 * @param <T> the type of the value
 */
public sealed interface Outcome<T extends @Nullable Object> {

  /**
   * The operation completed normally.
   *
   * @param value the value, null if the operation delivered null
   */
  record Success<T extends @Nullable Object>(T value) implements Outcome<T> {
  }

  /**
   * The operation failed.
   *
   * @param cause the exception, without the {@code CompletionException} wrapper that dependent stages add
   */
  record Failure<T extends @Nullable Object>(Throwable cause) implements Outcome<T> {
  }

  /**
   * @return whether the operation completed normally
   */
  default boolean isSuccess() {
    return this instanceof Success;
  }

  /**
   * Builds the outcome of a completed stage from the arguments of {@link CompletionStage#whenComplete}.
   *
   * @param value the value, if the stage completed normally
   * @param throwable the failure, or null if the stage completed normally; it is unwrapped with
   *     {@link Futures#unwrap}
   * @return a Success or a Failure
   */
  static <T extends @Nullable Object> Outcome<T> of(T value, @Nullable Throwable throwable) {
    if (throwable == null) {
      return new Success<T>(value);
    }
    return new Failure<T>(Futures.unwrap(throwable));
  }
}
