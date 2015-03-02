package edu.clarkson.cs.itop.core.model;

import scala.collection.JavaConversions.seqAsJavaList
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer

class NodeTest {

  var node1: Node = null;
  var node2: Node = null;
  var node3: Node = null;

  @Before
  def prepare: Unit = {
    node1 = new Node();
    node1.namedLinks = scala.collection.mutable.Map("1.1.1.1" -> new Link(1), "2.2.2.2" -> new Link(2), "3.3.3.3" -> new Link(3));
    node1.anonymousLinks ++= ListBuffer(new Link(4), new Link(5), new Link(6), new Link(7));

    node2 = new Node();
    node2.namedLinks = scala.collection.mutable.Map("1.1.1.1" -> new Link(1), "2.2.2.2" -> new Link(2), "3.3.3.3" -> new Link(3));

    node3 = new Node();
    node3.anonymousLinks ++= ListBuffer(new Link(4), new Link(5), new Link(6), new Link(7));

  }

  @Test
  def testCreateNode: Unit = {
    var ipList: java.util.List[String] = List("1", "2", "3", "4", "4", "5", "7");
    var node = new Node("N1", ipList);
    assertEquals(6, node.ips.size)
  }

  @Test
  def testAppendLink: Unit = {
    var node = new Node("N1", new java.util.ArrayList[String]());

    node.appendLink(new Link(1));
    node.appendLink(new Link(2));
    node.appendLink(new Link(3));

    node.appendLink(new Link(4), "1.3.4.5");
    node.appendLink(new Link(5), "2.3.4.5");
    node.appendLink(new Link(6), "3.3.4.5");
    node.appendLink(new Link(7), "4.3.4.5");

    assertEquals(3, node.anonymousLinks.size);
    assertEquals(4, node.namedLinks.size);
  }

  @Test
  def testForEachLink: Unit = {
    var set = scala.collection.mutable.Set[Int]();
    node1.links.foreach(f => { set += f._1.id; })
    assertEquals(7, set.size);

    set.clear;
    node2.links.foreach(f => { set += f._1.id; })
    assertEquals(3, set.size);

    set.clear;
    node3.links.foreach(f => { set += f._1.id; })
    assertEquals(4, set.size);

  }

  @Test
  def testLinkAtIndex: Unit = {
    var index = new Index();
    index.onNamedItems = true;
    index.nameKey = "1.1.1.1";
    var link = node1.links.at(index);
    assertEquals(1, link.id);

    index.onNamedItems = false;
    index.anonymousIndex = 2;
    link = node1.links.at(index);
    assertEquals(6, link.id);

    index.onNamedItems = true;
    index.nameKey = "2.2.2.2"
    link = node2.links.at(index);
    assertEquals(2, link.id);

    index.onNamedItems = false;
    index.anonymousIndex = 1;
    try {
      link = node2.links.at(index);
      fail("No such element")
    } catch {
      case e: NoSuchElementException => {}
    }

    index.onNamedItems = true;
    try {
      link = node3.links.at(index);
      fail("No such element");
    } catch {
      case e: NoSuchElementException => {}
    }
    index.onNamedItems = false;
    link = node3.links.at(index);
    assertEquals(5, link.id)
  }

  @Test
  def testFirstLink: Unit = {
    var tuple = node1.links.first;
    assertEquals(1, tuple._1.id);
    assertEquals(true, tuple._2.onNamedItems);
    assertEquals("1.1.1.1", tuple._2.nameKey);

    tuple = node2.links.first;
    assertEquals(1, tuple._1.id);
    assertEquals(true, tuple._2.onNamedItems);
    assertEquals("1.1.1.1", tuple._2.nameKey);

    tuple = node3.links.first;
    assertEquals(4, tuple._1.id);
    assertEquals(false, tuple._2.onNamedItems);
    assertEquals(0, tuple._2.anonymousIndex);
  }

  @Test
  def testNextLink: Unit = {
    var tuple = node1.links.next(node1.links.first._2)
    assertEquals(2, tuple._1.id)
    tuple = node1.links.next(tuple._2)
    assertEquals(3, tuple._1.id)
    tuple = node1.links.next(tuple._2)
    assertEquals(4, tuple._1.id)
    tuple = node1.links.next(tuple._2)
    assertEquals(5, tuple._1.id)
    tuple = node1.links.next(tuple._2)
    assertEquals(6, tuple._1.id)
    tuple = node1.links.next(tuple._2)
    assertEquals(7, tuple._1.id)
    try {
      tuple = node1.links.next(tuple._2)
      fail("No such element")
    } catch {
      case e: NoSuchElementException => {

      }
    }
  }
}