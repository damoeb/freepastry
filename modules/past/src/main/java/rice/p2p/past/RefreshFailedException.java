package rice.p2p.past;

/**
 * Reported per key by {@code GCPast.refresh} when the refresh failed for a reason other than a missing object, for
 * example because no replica answered.
 */
public class RefreshFailedException extends PastException {

  public RefreshFailedException(String msg) {
    super(msg);
  }
}
