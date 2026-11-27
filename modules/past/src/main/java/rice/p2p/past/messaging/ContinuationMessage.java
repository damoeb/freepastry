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

package rice.p2p.past.messaging;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.past.PastRemoteException;

/**
 * This class the abstraction of a message used internally by Past which serves
 * as a continuation (for receiving the results of an operation).
 *
 * @version $Id: ContinuationMessage.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
@SuppressWarnings("unchecked")
public sealed abstract class ContinuationMessage extends PastMessage implements Continuation<Object, Exception>
    permits FetchMessage, FetchHandleMessage, InsertMessage, LookupHandlesMessage, LookupMessage, GCRefreshMessage {

  // the response data
  protected Object response;

  // the response exception, if one is thrown
  private Exception exception;

  /**
    * Constructor which takes a unique integer Id, as well as the
   * data to be stored
   *
   * @param uid The unique id
   * @param source The source handle
   * @param dest The destination address
   */
  protected ContinuationMessage(long uid, NodeHandle source, Id dest) {
    super(uid, source, dest);
  }

  /**
    * Method which builds a response for this message, using the provided
   * object as a result.
   *
   * @param result The object argument
   */
  @Override
  public void receiveResult(Object result) {
    setResponse();
    response = result;
  }

  /**
    * Method which builds a response for this message, using the provided
   * exception, which was thrown
   *
   * @param e The exception argument
   */
  @Override
  public void receiveException(Exception e) {
    setResponse();
    exception = e;
  }

  /**
    * Method by which this message is supposed to return it's response.
   *
   * @param request The future of the request this message answers.
   */
  @Override
  public void returnResponse(CompletableFuture<Object> request, Environment env, String instance) {
    if (exception == null)
      request.complete(response);
    else
      request.completeExceptionally(exception);
  }

  /**
   * No response or exception.
   */
  public static final byte S_EMPTY = 0;
  /**
   * The subclass wrote the response.
   */
  public static final byte S_SUB = 1;
  /**
   * An exception: its class name and its message.
   */
  public static final byte S_EXCEPTION = 2;

  /**
   * The subclass writes and reads the response; this class writes the {@code S_*} byte and an exception.
   *
   */
  public ContinuationMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
    super(buf, endpoint);
    serType = buf.readByte();
    if (serType == S_EXCEPTION) {
      String className = buf.readUTF();
      String message = buf.readBoolean() ? buf.readUTF() : null;
      exception = new PastRemoteException(className, message);
    } else if (serType > S_EXCEPTION) {
      throw new IOException("Unknown response encoding " + serType);
    }
  }

  @Override
  public abstract void serialize(OutputBuffer buf) throws IOException;

  protected byte serType;

  /**
   * Writes the header and the {@code S_*} byte; with {@code S_SUB} the subclass writes the response next.
   * An exception is sent as its class name and message, and arrives as a {@link PastRemoteException}.
   *
   * @param subclassWritesResponse whether the response is of the type the subclass serializes
   * @throws IOException if there is a response the subclass cannot serialize
   */
  public void serialize(OutputBuffer buf, boolean subclassWritesResponse) throws IOException {
    super.serialize(buf);

    if (subclassWritesResponse) {
      serType = S_SUB;
      buf.writeByte(serType);
    } else if (exception != null) {
      serType = S_EXCEPTION;
      buf.writeByte(serType);
      buf.writeUTF(exception instanceof PastRemoteException remote ? remote.getRemoteClassName() : exception.getClass().getName());
      String message = exception instanceof PastRemoteException remote ? remote.getRemoteMessage() : exception.getMessage();
      buf.writeBoolean(message != null);
      if (message != null) {
        buf.writeUTF(message);
      }
    } else if (response == null) {
      serType = S_EMPTY;
      buf.writeByte(serType);
    } else {
      throw new IOException("Cannot serialize the response " + response + " of " + response.getClass().getName() + " in " + this);
    }
  }
}
