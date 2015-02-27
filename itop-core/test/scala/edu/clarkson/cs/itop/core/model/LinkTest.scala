package edu.clarkson.cs.itop.core.model

import org.junit.Test
import org.junit.Assert._
import scala.collection.JavaConversions._

class LinkTest {

  @Test
  def testConstructor: Unit = {
    var link = new Link("12", List(("N1", "3.4.5.6"), ("N2", ""), ("N3", ""), ("N4", "2.2.2.2")))
    assertEquals(2, link.namedNodeIds.size)
    assertEquals(2, link.anonymousNodeIds.size)
    assertEquals(1, link.namedNodeIds.get("3.4.5.6").get)
    assertEquals(4, link.namedNodeIds.get("2.2.2.2").get)
    assertTrue(link.anonymousNodeIds.contains(2))
    assertTrue(link.anonymousNodeIds.contains(3))
  }

  @Test
  def testAttachNodes: Unit = {
    var link = new Link(1);
    link.namedNodeIds += { "1.2.3.4" -> 1; "2.3.4.5" -> 2; "3.4.5.6" -> 3 }
    link.anonymousNodeIds ++= List(5, 6, 7, 8);
  }

  @Test
  def testNodeSize: Unit = {

  }

  @Test
  def testForeachNode: Unit = {

  }

  @Test
  def testNodeAtIndex: Unit = {

  }

  @Test
  def testNextNode: Unit = {

  }
}