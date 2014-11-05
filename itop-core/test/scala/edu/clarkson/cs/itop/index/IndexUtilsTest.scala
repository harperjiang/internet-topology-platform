package edu.clarkson.cs.itop.index;

import org.junit.Assert._
import org.junit.Test

import edu.clarkson.cs.itop.index.IndexUtils;
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer

class IndexUtilsTest {

  @Test
  def testBsearch() {
    var b = List(1, 3, 4, 6, 29, 71, 85, 99, 111, 134, 271, 350).toBuffer;
    var comp = (a: Int, target: Int) => a - target;
    assertEquals(9, IndexUtils.bsearch[Int](b, 134, comp))
    assertEquals(3, IndexUtils.bsearch[Int](b, 6, comp))
    assertEquals(11, IndexUtils.bsearch[Int](b, 350, comp))
    assertEquals(0, IndexUtils.bsearch[Int](b, 1, comp))
    assertEquals(-1, IndexUtils.bsearch[Int](b, 225, comp))

  }

}
