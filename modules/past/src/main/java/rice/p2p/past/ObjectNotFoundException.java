package rice.p2p.past;

/**
 * Reported per key by {@code GCPast.refresh} when no replica holds an object under the key.
 */
public class ObjectNotFoundException extends PastException {

  public ObjectNotFoundException(String msg) {
    super(msg);
  }
}
