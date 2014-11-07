package edu.clarkson.cs.itop.core.conhash

import scala.BigDecimal

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test

class TreeMapHashCircleTest {
  var emptyCircle: TreeMapHashCircle = null;
  var circle1: TreeMapHashCircle = null;
  var circle2: TreeMapHashCircle = null;

  @Before
  def prepare = {

    emptyCircle = new TreeMapHashCircle();

    circle1 = new TreeMapHashCircle();
    circle1.insert(List(BigDecimal(1), BigDecimal(4), BigDecimal(9)), "node1");
    circle1.insert(List(BigDecimal(2), BigDecimal(3), BigDecimal(12)), "node2");
    circle1.insert(List(BigDecimal(0.5), BigDecimal(3.7), BigDecimal(21)), "node3");
    circle1.insert(List(BigDecimal(3.75), BigDecimal(2.68), BigDecimal(1.31)), "node4");
  }

  @Test
  def testInsert = {
    circle1.insert(List(BigDecimal(32.42323)), "test");
    try {
      circle1.insert(List(BigDecimal(0.5)), "test2");
      fail("No detection of duplicate key");
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test
  def testRemove = {
    assertTrue(emptyCircle.remove("node1").isEmpty);

    var node1 = circle1.remove("node1");
    assertTrue(circle1.remove("node1").isEmpty);
    assertEquals(3, node1.size);
    assertTrue(node1.exists(_ == ("node1", BigDecimal(1))));
    assertTrue(node1.exists(_ == ("node1", BigDecimal(4))));
    assertTrue(node1.exists(_ == ("node1", BigDecimal(9))));

    var node2 = circle1.remove("node2");
    assertTrue(circle1.remove("node2").isEmpty);
    assertEquals(3, node2.size);
    assertTrue(node2.exists(_ == ("node2", BigDecimal(2))));
    assertTrue(node2.exists(_ == ("node2", BigDecimal(3))));
    assertTrue(node2.exists(_ == ("node2", BigDecimal(12))));
  }

  @Test
  def testBefore = {
    assertEquals(None, emptyCircle.before(BigDecimal(5)));

    assertEquals(Some("node3", BigDecimal(21)), circle1.before(BigDecimal(0.0001)));
    assertEquals(Some("node2", BigDecimal(2)), circle1.before(BigDecimal(2.5)));
    assertEquals(Some("node1", BigDecimal(4)), circle1.before(BigDecimal(4.5324)));
    assertEquals(Some("node3", BigDecimal(21)), circle1.before(BigDecimal(52.23423)));
  }

  @Test
  def testAfter = {
    assertEquals(None, emptyCircle.afterOrOn(BigDecimal(5)));

    assertEquals(Some("node3", BigDecimal(21)), circle1.afterOrOn(BigDecimal(0.0001)));
    assertEquals(Some("node2", BigDecimal(2)), circle1.afterOrOn(BigDecimal(2.5)));
    assertEquals(Some("node1", BigDecimal(4)), circle1.afterOrOn(BigDecimal(4.5324)));
    assertEquals(Some("node3", BigDecimal(21)), circle1.afterOrOn(BigDecimal(52.23423)));
  }

  @Test
  def testFind = {

  }

  @Test
  def testToList = {

  }
}