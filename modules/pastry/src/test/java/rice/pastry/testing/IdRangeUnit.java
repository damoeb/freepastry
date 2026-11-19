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
package rice.pastry.testing;

import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.pastry.Id;
import rice.pastry.IdRange;

/**
 * IdRangeUnit tests the IdRange class.
 *
 * @version $Id: IdRangeUnit.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Peter Druschel
 */

public class IdRangeUnit {
  private RandomSource random;

  public IdRange createRandomIdRange() {
    IdRange range;
    range = new IdRange(Id.makeRandomId(random), Id.makeRandomId(random));
    return range;
  }

  public IdRange createFullIdRange() {
    IdRange range;
    Id id = Id.makeRandomId(random);
    range = new IdRange(id, id);
    return range;
  }

  public IdRange createEmptyIdRange() {
    IdRange range;
    range = new IdRange();
    return range;
  }

  public IdRange createIdRangeStartingWith(Id counterClockwiseId) {
    IdRange range;
    range = new IdRange(counterClockwiseId, Id.makeRandomId(random));
    return range;
  }

  public IdRange createIdRangeEndingIn(Id clockwiseId) {
    IdRange range;
    range = new IdRange(Id.makeRandomId(random), clockwiseId);
    return range;
  }

  public Id createIdWithPrefix(byte b) {
    byte[] data = new byte[Id.IdBitLength / 8];

    data[data.length - 1] = b;
    return Id.build(data);
  }

  public void halfTest() {
    IdRange range = createFullIdRange();
    IdRange counterClockwiseRange = range.counterClockwiseHalf();
    IdRange clockwiseRange = range.clockwiseHalf();

    if (!clockwiseRange.getClockwiseBound().distance(clockwiseRange.getCounterClockwiseBound()).equals(clockwiseRange.getClockwiseBound().longDistance(clockwiseRange.getCounterClockwiseBound()))) {
      System.out.println("ALERT: cw long distance not equal");
    }

    if (!counterClockwiseRange.getClockwiseBound().distance(counterClockwiseRange.getCounterClockwiseBound()).equals(counterClockwiseRange.getClockwiseBound().longDistance(counterClockwiseRange.getCounterClockwiseBound()))) {
      System.out.println("ALERT: ccw long distance not equal");
    }
  }

  @SuppressWarnings("SelfEquals") // r1.equals(r1) checks that equals is reflexive
  public void equalityTest(IdRange firstRange, IdRange secondRange) {

    if (firstRange.equals(secondRange) && !firstRange.isEmpty() && !firstRange.isFull())
      System.out.println("ALERT: equality failed with high probability" + firstRange
          + secondRange);

    if (!firstRange.equals(firstRange))
      System.out.println("ALERT: equality is not reflexive." + firstRange);

    IdRange r3 = new IdRange(secondRange.getCounterClockwiseBound(), secondRange.getClockwiseBound());
    IdRange r31 = new IdRange(r3);

    if ((!r3.equals(secondRange) && !secondRange.isEmpty()) || !r3.equals(r31))
      System.out.println("ALERT: equality failed." + secondRange + r3 + r31);

    IdRange r4 = new IdRange();
    IdRange r5 = new IdRange(firstRange.getCounterClockwiseBound(), firstRange.getCounterClockwiseBound());

    if (r4.equals(r5))
      System.out.println("ALERT: equality failed" + r4 + r4);
    if (!r4.isEmpty() || r5.isEmpty())
      System.out.println("ALERT: isEmpty failed" + r4 + r5);

  }

  public void mergeIntersectTest(IdRange firstRange, IdRange secondRange) {

    IdRange m1 = firstRange.merge(secondRange);
    IdRange m2 = secondRange.merge(firstRange);
    IdRange i1 = firstRange.intersect(secondRange);

    if (!m1.equals(m2) && (!i1.isEmpty() || firstRange.isAdjacent(secondRange)) && !firstRange.isEmpty())
      System.out.println("ALERT: merge is not symmetric 1" + firstRange + secondRange + m1 + m2);

    boolean intersect = !i1.isEmpty();
    boolean adjacent = firstRange.isAdjacent(secondRange);
    boolean intersectOrAdjacent = intersect || adjacent;

    IdRange i2 = secondRange.intersect(firstRange);

    if (i1.isEmpty() != i2.isEmpty())
      System.out.println("ALERT: intersect error 1." + i1 + i2);

    if (intersectOrAdjacent) {

      if (m1.equals(firstRange) && !i1.isEmpty() && !i1.equals(secondRange))
        System.out.println("ALERT: merge is not symmetric 2" + firstRange + secondRange + m1
            + i1);

      if (m1.equals(secondRange) && !i1.isEmpty() && !i1.equals(firstRange))
        System.out.println("ALERT: merge is not symmetric 3" + firstRange + secondRange + m1
            + i1);

      IdRange re1 = m1.intersect(firstRange);
      IdRange re2 = m1.intersect(secondRange);

      if (!firstRange.equals(re1) || !secondRange.equals(re2))
        System.out.println("ALERT: intersect error 2." + firstRange + re1 + secondRange + re2
            + m1);

    } else {
      if (!m1.equals(firstRange) && !firstRange.isEmpty())
        System.out.println("ALERT: merge error 0." + firstRange + secondRange + m1);
    }

    if ((firstRange.isEmpty() || secondRange.isEmpty()) && !i1.isEmpty())
      System.out.println("ALERT: intersect error 3." + firstRange + secondRange + i1);

    if (firstRange.isEmpty() && !m1.equals(secondRange))
      System.out.println("ALERT: merge error 1." + firstRange + secondRange + m1);
    if (secondRange.isEmpty() && !m1.equals(firstRange))
      System.out.println("ALERT: merge error 2." + firstRange + secondRange + m1);

    if ((firstRange.isFull() || secondRange.isFull()) && !m1.isFull())
      System.out.println("ALERT: merge error 3." + firstRange + secondRange + m1);

    if (firstRange.isFull() && !i1.equals(secondRange))
      System.out.println("ALERT: intersect error 4." + firstRange + secondRange + i1);
    if (secondRange.isFull() && !i1.equals(firstRange))
      System.out.println("ALERT: intersect error 5." + firstRange + secondRange + m1);

  }

  public void diffSubtractTest(IdRange firstRange, IdRange secondRange) {

    IdRange d1 = firstRange.diff(secondRange);
    IdRange d2 = secondRange.diff(firstRange);

    IdRange i1 = firstRange.intersect(secondRange);
    IdRange i2 = secondRange.intersect(firstRange);

    if (!d1.intersect(i1).isEmpty() || !d1.intersect(i2).isEmpty()
        || !d2.intersect(i1).isEmpty() || !d2.intersect(i2).isEmpty())
      System.out.println("ALERT: diff error 1." + firstRange + secondRange + d1 + d2 + i1 + i2);

    if (!i1.equals(i2)) {
      if (d1.equals(d2)) {
        System.out.println("ALERT: diff error 15." + firstRange + secondRange + d1 + d2 + i1
            + i2);
      }
    }

    // try to reconstitute
    IdRange re1 = d1;
    IdRange re2 = d2;
    for (int i = 0; i < 3; i++) {
      re1 = re1.merge(d1);
      re1 = re1.merge(i1);
      re1 = re1.merge(d2);
      re1 = re1.merge(i2);
      re2 = re2.merge(d1);
      re2 = re2.merge(i1);
      re2 = re2.merge(d2);
      re2 = re2.merge(i2);
    }
    IdRange r12 = firstRange.merge(secondRange);

    if ((!re1.equals(firstRange) && !re1.equals(secondRange) && !re1.equals(r12))
        || (!re2.equals(firstRange) && !re2.equals(secondRange) && !re2.equals(r12)))
      System.out.println("ALERT: diff error 2." + firstRange + secondRange + d1 + d2 + i1 + i2
          + re1 + re2);

    d1 = firstRange.subtract(secondRange, false);
    d2 = firstRange.subtract(secondRange, true);

    if (!d1.intersect(i1).isEmpty() || !d1.intersect(i2).isEmpty()
        || !d2.intersect(i1).isEmpty() || !d2.intersect(i2).isEmpty())
      System.out.println("ALERT: subtract error 1." + firstRange + secondRange + d1 + d2 + i1
          + i2);

    if ((!d1.isEmpty() && d1.intersect(firstRange).isEmpty())
        || (!d2.isEmpty() && d2.intersect(firstRange).isEmpty()))
      System.out.println("ALERT: subtract error 2." + firstRange + secondRange + d1 + d2);

    // try to reconstitute
    re1 = d1;
    re2 = d2;
    for (int i = 0; i < 3; i++) {
      re1 = re1.merge(d1);
      re1 = re1.merge(i1);
      re1 = re1.merge(d2);
      re1 = re1.merge(i2);
      re2 = re2.merge(d1);
      re2 = re2.merge(i1);
      re2 = re2.merge(d2);
      re2 = re2.merge(i2);
    }

    if (!re1.equals(firstRange) && !re2.equals(firstRange))
      System.out.println("ALERT: subtract error 25." + firstRange + secondRange + d1 + d2 + i1
          + i2 + re1 + re2);

    d1 = secondRange.subtract(firstRange, false);
    d2 = secondRange.subtract(firstRange, true);

    if (!d1.intersect(i1).isEmpty() || !d1.intersect(i2).isEmpty()
        || !d2.intersect(i1).isEmpty() || !d2.intersect(i2).isEmpty())
      System.out.println("ALERT: subtract error 3." + firstRange + secondRange + d1 + d2 + i1
          + i2);

    if ((!d1.isEmpty() && d1.intersect(secondRange).isEmpty())
        || (!d2.isEmpty() && d2.intersect(secondRange).isEmpty()))
      System.out.println("ALERT: subtract error 4." + firstRange + secondRange + d1 + d2);

    // try to reconstitute
    re1 = d1;
    re2 = d2;
    for (int i = 0; i < 3; i++) {
      re1 = re1.merge(d1);
      re1 = re1.merge(i1);
      re1 = re1.merge(d2);
      re1 = re1.merge(i2);
      re2 = re2.merge(d1);
      re2 = re2.merge(i1);
      re2 = re2.merge(d2);
      re2 = re2.merge(i2);
    }

    if (!re1.equals(secondRange) && !re2.equals(secondRange))
      System.out.println("ALERT: subtract error 5." + firstRange + secondRange + d1 + d2 + i1
          + i2 + re1 + re2);

  }

  public void mergeTest(IdRange firstRange, IdRange secondRange) {

    IdRange counterClockwiseRange = firstRange.counterClockwiseHalf();
    IdRange clockwiseRange = firstRange.clockwiseHalf();

    if (!counterClockwiseRange.intersect(clockwiseRange).isEmpty())
      System.out.println("ALERT: ccw and cw halves intersect." + firstRange + counterClockwiseRange + clockwiseRange);
    if (!firstRange.equals(counterClockwiseRange.merge(clockwiseRange)))
      System.out.println("ALERT: merge cw/ccw failure." + firstRange + counterClockwiseRange + clockwiseRange);

    counterClockwiseRange = secondRange.counterClockwiseHalf();
    clockwiseRange = secondRange.clockwiseHalf();

    if (!counterClockwiseRange.intersect(clockwiseRange).isEmpty())
      System.out.println("ALERT: ccw and cw halves intersect." + secondRange + counterClockwiseRange + clockwiseRange);
    if (!secondRange.equals(counterClockwiseRange.merge(clockwiseRange)))
      System.out.println("ALERT: merge cw/ccw failure." + secondRange + counterClockwiseRange + clockwiseRange);

  }

  public IdRangeUnit() {
    random = new SimpleRandomSource(null);

    System.out.println("IdRangeUnit test starting...");

    halfTest();

    for (int i = 0; i < 1000; i++) {
      IdRange firstRange = createEmptyIdRange();
      IdRange secondRange = createEmptyIdRange();

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createFullIdRange();

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createRandomIdRange();

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      //
      firstRange = createFullIdRange();

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createFullIdRange();

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createEmptyIdRange();

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      //
      firstRange = createRandomIdRange();

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeStartingWith(firstRange.getClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeStartingWith(firstRange.getCounterClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeEndingIn(firstRange.getClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeEndingIn(firstRange.getCounterClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = firstRange.complement();

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      //
      firstRange = createFullIdRange();

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeStartingWith(firstRange.getClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeStartingWith(firstRange.getCounterClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeEndingIn(firstRange.getClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeEndingIn(firstRange.getCounterClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      //
      firstRange = createEmptyIdRange();

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeStartingWith(firstRange.getClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeStartingWith(firstRange.getCounterClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeEndingIn(firstRange.getClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      secondRange = createIdRangeEndingIn(firstRange.getCounterClockwiseBound());

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);

      firstRange = new IdRange(createIdWithPrefix((byte) 0xC0),
          createIdWithPrefix((byte) 0x40));
      secondRange = new IdRange(createIdWithPrefix((byte) 0xE0),
          createIdWithPrefix((byte) 0x20));

      equalityTest(firstRange, secondRange);
      mergeIntersectTest(firstRange, secondRange);
      diffSubtractTest(firstRange, secondRange);
      mergeTest(firstRange, secondRange);
    }

    System.out.println("IdRangeUnit test finished.");

  }

  public static void main(String args[]) {
    new IdRangeUnit(); // the constructor runs the tests
  }
}
