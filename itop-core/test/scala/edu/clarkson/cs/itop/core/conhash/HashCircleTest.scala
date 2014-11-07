package edu.clarkson.cs.itop.core.conhash

import scala.BigDecimal
import scala.collection.JavaConversions._

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

    circle2 = new TreeMapHashCircle();
    circle2.insert(List(BigDecimal(4), BigDecimal(35)), "node1")
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
    assertEquals(None, emptyCircle.after(BigDecimal(5)));

    assertEquals(Some("node3", BigDecimal(0.5)), circle1.after(BigDecimal(0.0001)));
    assertEquals(Some("node4", BigDecimal(2.68)), circle1.after(BigDecimal(2.5)));
    assertEquals(Some("node1", BigDecimal(9)), circle1.after(BigDecimal(4.5324)));
    assertEquals(Some("node3", BigDecimal(0.5)), circle1.after(BigDecimal(52.23423)));
  }

  @Test
  def testFind = {
    assertTrue(emptyCircle.find(BigDecimal(5), 3).isEmpty);

    var fromcircle1 = circle1.find(BigDecimal(3.242), 4);

    assertEquals(4, fromcircle1.size);
    assertTrue(fromcircle1.exists(_ == ("node3", BigDecimal(3.7))));
    assertTrue(fromcircle1.exists(_ == ("node4", BigDecimal(3.75))));
    assertTrue(fromcircle1.exists(_ == ("node1", BigDecimal(4))));
    assertTrue(fromcircle1.exists(_ == ("node1", BigDecimal(9))));

    var fromcircle2 = circle2.find(BigDecimal(3.242), 4);

    assertEquals(2, fromcircle2.size);
    assertTrue(fromcircle2.exists(_ == ("node1", BigDecimal(4))));
    assertTrue(fromcircle2.exists(_ == ("node1", BigDecimal(35))));
  }

  @Test
  def testToList = {
    assertTrue(emptyCircle.content.isEmpty());
    var fc1 = circle1.content
    var fc2 = circle2.content
    
    assertEquals(4,fc1.size())
    assertEquals(1,fc2.size());
    
    assertTrue(fc1.exists(_=="node1"))
    assertTrue(fc1.exists(_=="node2"))
    assertTrue(fc1.exists(_=="node3"))
    assertTrue(fc1.exists(_=="node4"))
  
    assertTrue(fc2.exists(_=="node1"))
  }
}