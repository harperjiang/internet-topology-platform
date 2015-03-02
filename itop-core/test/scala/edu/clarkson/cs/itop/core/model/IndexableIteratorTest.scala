package edu.clarkson.cs.itop.core.model

import org.junit.Test
import org.junit.Assert._
import org.junit.Before
import scala.collection.immutable.TreeMap

class IndexableIteratorTest {

  private var iterator1: IndexableIterator[String] = null;
  private var iterator2: IndexableIterator[String] = null;
  private var iterator3: IndexableIterator[String] = null;
  private var iterator4: IndexableIterator[String] = null;

  @Before
  def prepare: Unit = {
    iterator1 = new IndexableIterator[String](TreeMap("1" -> "1", "2" -> "2", "3" -> "3", "4" -> "4"), List("10", "20", "30", "40", "50"));
    iterator2 = new IndexableIterator[String](TreeMap("1" -> "1", "2" -> "2", "3" -> "3", "4" -> "4"), List());
    iterator3 = new IndexableIterator[String](TreeMap(), List("10", "20", "30", "40", "50"));
    iterator4 = new IndexableIterator[String](TreeMap(), List());
  }

  @Test
  def testHasNext: Unit = {
    assertTrue(iterator1.hasNext);
    assertTrue(iterator2.hasNext);
    assertTrue(iterator3.hasNext);
    assertFalse(iterator4.hasNext);
  }

  @Test
  def testIsEmpty: Unit = {
    assertFalse(iterator1.isEmpty);
    assertFalse(iterator2.isEmpty);
    assertFalse(iterator3.isEmpty);
    assertTrue(iterator4.isEmpty);
  }

  @Test
  def testNext: Unit = {
    assertEquals("1", iterator1.next())
    assertEquals("2", iterator1.next())
    assertEquals("3", iterator1.next())
    assertEquals("4", iterator1.next())
    assertEquals("10", iterator1.next())
    assertEquals("20", iterator1.next())
    assertEquals("30", iterator1.next())
    assertEquals("40", iterator1.next())
    assertEquals("50", iterator1.next())
    try {
      iterator1.next
      fail
    } catch {
      case e: NoSuchElementException => {}
    }
    assertEquals("1", iterator2.next())
    assertEquals("2", iterator2.next())
    assertEquals("3", iterator2.next())
    assertEquals("4", iterator2.next())
    try {
      iterator2.next
      fail
    } catch {
      case e: NoSuchElementException => {}
    }
    assertEquals("10", iterator3.next())
    assertEquals("20", iterator3.next())
    assertEquals("30", iterator3.next())
    assertEquals("40", iterator3.next())
    assertEquals("50", iterator3.next())
    try {
      iterator3.next
      fail
    } catch {
      case e: NoSuchElementException => {}
    }

    try {
      iterator4.next
      fail
    } catch {
      case e: NoSuchElementException => {}
    }
  }

  @Test
  def testTo: Unit = {
    var index = new Index();
    index.onNamedItems = true;
    index.nameKey = "4"

    iterator1.to(index);
    assertEquals("10", iterator1.next)
  }

  @Test
  def testForeach: Unit = {
    var set = scala.collection.mutable.Set[String]();
    iterator1.foreach(f => set.add(f._1));
    assertEquals(9, set.size)
    assertTrue(set.contains("1"))
    assertTrue(set.contains("2"))
    assertTrue(set.contains("3"))
    assertTrue(set.contains("4"))
    assertTrue(set.contains("10"))
    assertTrue(set.contains("20"))
    assertTrue(set.contains("30"))
    assertTrue(set.contains("40"))
    assertTrue(set.contains("50"))
  }
  /*
  @Test
  def testNodeAtIndex: Unit = {
    var set = scala.collection.mutable.Set[Int]();
    var link = new Link(1)
    link.namedNodeIds ++= Map("1.2.3.4" -> 1, "2.3.4.5" -> 2, "3.4.5.6" -> 3)
    link.anonymousNodeIds ++= List(5, 6, 7, 8);
    var nodeMap = Map(1 -> new Node(), 2 -> new Node(), 3 -> new Node(), 5 -> new Node(), 6 -> new Node(), 7 -> new Node(), 8 -> new Node)
    link.attachNodes(nodeMap);

    var index = new Index()
    index.onNamedItems = true
    index.nameKey = "1.2.3.4"
    assertEquals(nodeMap.get(1).get, link.nodes.at(index))

    var index2 = new Index()
    index2.onNamedItems = false
    index2.anonymousIndex = 0
    assertEquals(nodeMap.get(5).get, link.nodes.at(index2))
  }

  @Test
  def testFirstNode: Unit = {
    var link = new Link(1)
    link.namedNodeIds ++= Map("1.2.3.4" -> 1, "2.3.4.5" -> 2, "3.4.5.6" -> 3)
    link.anonymousNodeIds ++= List(5, 6, 7, 8);
    var nodeMap = Map(1 -> new Node(), 2 -> new Node(), 3 -> new Node(), 5 -> new Node(), 6 -> new Node(), 7 -> new Node(), 8 -> new Node)
    link.attachNodes(nodeMap);
    assertEquals(nodeMap.get(1).get, link.nodes.first._1)

    var link2 = new Link(2)
    link2.anonymousNodeIds ++= List(5, 6, 7, 8);
    var nodeMap2 = Map(1 -> new Node(), 2 -> new Node(), 3 -> new Node(), 5 -> new Node(), 6 -> new Node(), 7 -> new Node(), 8 -> new Node)
    link2.attachNodes(nodeMap2);
    assertEquals(nodeMap2.get(5).get, link2.nodes.first._1)
  }

  @Test
  def testNextNode: Unit = {
    var link = new Link(1)
    link.namedNodeIds ++= Map("1.2.3.4" -> 1, "2.3.4.5" -> 2, "3.4.5.6" -> 3)
    link.anonymousNodeIds ++= List(5, 6, 7, 8);
    var nodeMap = Map(1 -> new Node(), 2 -> new Node(), 3 -> new Node(), 5 -> new Node(), 6 -> new Node(), 7 -> new Node(), 8 -> new Node)
    link.attachNodes(nodeMap);

    var index = link.nodes.first._2;
    var tuple = link.nodes.next(index);
    index = tuple._2;
    assertEquals(nodeMap.get(2).get, tuple._1)
    tuple = link.nodes.next(index);
    index = tuple._2;
    assertEquals(nodeMap.get(3).get, tuple._1)
    tuple = link.nodes.next(index);
    index = tuple._2;
    assertEquals(nodeMap.get(5).get, tuple._1)
    tuple = link.nodes.next(index);
    index = tuple._2;
    assertEquals(nodeMap.get(6).get, tuple._1)
    tuple = link.nodes.next(index);
    index = tuple._2;
    assertEquals(nodeMap.get(7).get, tuple._1)
    tuple = link.nodes.next(index);
    index = tuple._2;
    assertEquals(nodeMap.get(8).get, tuple._1)
    try {
      tuple = link.nodes.next(index);
      fail("No such Element");
    } catch {
      case e: NoSuchElementException => {

      }
    }
  }*/
}