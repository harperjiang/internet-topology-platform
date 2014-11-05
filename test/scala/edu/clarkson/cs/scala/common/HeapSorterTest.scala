package edu.clarkson.cs.itop.tool

import org.scalatest.FunSuite
import edu.clarkson.cs.scala.common.HeapSorter

class HeapSorterTest extends FunSuite {
  test("Test Sort Integers") {
    var sorter = new HeapSorter();
    var result = sorter.sort(List[Int](3, 523, 15, 13, 1, 14, 135, 13), (a: Int, b: Int) => {
      a - b;
    });
    assert(8 == result.length)
    assert(1 == result(0))
    assert(3 == result(1))
    assert(13 == result(2))
    assert(13 == result(3))
    assert(14 == result(4))
    assert(15 == result(5))
    assert(135 == result(6))
    assert(523 == result(7))
  }
}