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
    link.namedNodeIds ++= Map("1.2.3.4" -> 1, "2.3.4.5" -> 2, "3.4.5.6" -> 3)
    link.anonymousNodeIds ++= List(5, 6, 7, 8);

    var nodeMap = Map(1 -> new Node(), 2 -> new Node(), 3 -> new Node(), 5 -> new Node(), 6 -> new Node(), 7 -> new Node(), 8 -> new Node)
    link.attachNodes(nodeMap);

    assertEquals(nodeMap.get(1).get, link.namedNodes.get("1.2.3.4").get)
    assertEquals(nodeMap.get(2).get, link.namedNodes.get("2.3.4.5").get)
    assertEquals(nodeMap.get(3).get, link.namedNodes.get("3.4.5.6").get)
    assertEquals(nodeMap.get(5).get, link.anonymousNodes(0))
    assertEquals(nodeMap.get(6).get, link.anonymousNodes(1))
    assertEquals(nodeMap.get(7).get, link.anonymousNodes(2))
    assertEquals(nodeMap.get(8).get, link.anonymousNodes(3))
  }

  @Test
  def testNodeSize: Unit = {
    var link = new Link("12", List(("N1", "3.4.5.6"), ("N2", ""), ("N3", ""), ("N4", "2.2.2.2")))
    assertEquals(4, link.nodeSize)
  }

  @Test
  def testForeachNode: Unit = {
    var set = scala.collection.mutable.Set[Int]();
    var link = new Link(1)
    link.namedNodeIds ++= Map("1.2.3.4" -> 1, "2.3.4.5" -> 2, "3.4.5.6" -> 3)
    link.anonymousNodeIds ++= List(5, 6, 7, 8);
    var nodeMap = Map(1 -> new Node(1), 2 -> new Node(2), 3 -> new Node(3), 5 -> new Node(5), 6 -> new Node(6), 7 -> new Node(7), 8 -> new Node(8))
    link.attachNodes(nodeMap);
    link.nodes.foreach(f => {
      set.add(f._1.id);
    });

    assertEquals(7, set.size);
    assertTrue(set.contains(1))
    assertTrue(set.contains(2))
    assertTrue(set.contains(3))
    assertTrue(set.contains(5))
    assertTrue(set.contains(6))
    assertTrue(set.contains(7))
    assertTrue(set.contains(8))
  }

}