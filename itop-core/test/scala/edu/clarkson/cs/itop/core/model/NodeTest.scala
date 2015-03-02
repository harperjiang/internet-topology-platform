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
}