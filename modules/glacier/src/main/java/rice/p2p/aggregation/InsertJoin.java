package rice.p2p.aggregation;

import rice.Continuation;

/**
 * Joins the two inserts of a non-aggregated object (object store and aggregate store): reports success once both
 * succeeded, and reports failure once, for the first failure.
 */
final class InsertJoin implements Continuation<Boolean[], Exception> {
  private static final int EXPECTED_RESULTS = 2;

  private final Continuation<? super Boolean[], Exception> command;
  private int succeeded = 0;
  private boolean reported = false;

  InsertJoin(Continuation<? super Boolean[], Exception> command) {
    this.command = command;
  }

  @Override
  public void receiveResult(Boolean[] results) {
    boolean complete;
    synchronized (this) {
      succeeded++;
      complete = (succeeded == EXPECTED_RESULTS) && !reported;
      reported |= complete;
    }
    if (complete)
      command.receiveResult(new Boolean[] {Boolean.TRUE});
  }

  @Override
  public void receiveException(Exception e) {
    boolean first;
    synchronized (this) {
      first = !reported;
      reported = true;
    }
    if (first)
      command.receiveException(e);
  }
}
