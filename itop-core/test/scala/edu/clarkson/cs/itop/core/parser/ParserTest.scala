package edu.clarkson.cs.itop.core.parser;

import org.junit.Assert._
import org.junit.Test;

import edu.clarkson.cs.itop.core.model.NodeLink
import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.model.Node


class ParserTest {

  @Test
  def testParseNode() = {
    var parser = new Parser();
    var node = parser.parse[Node]("node N13: 123.323.232.133 123.313.144.131");
    assertEquals(13, node.id);
    assertEquals(2, node.ips.size);
    assertTrue(node.ips.contains("123.323.232.133"));
    assertTrue(node.ips.contains("123.313.144.131"));
  }

  @Test
  def testParseLink() = {
    var parser = new Parser();
    var link = parser
      .parse[Link]("link L174: N13:13.42.23.23 N25:323.223.24.14 N16 N71");
    assertEquals(174, link.id);
    assertEquals(4, link.nodeSize);
    assertEquals(13, link.namedNodeIds.get("13.42.23.23").get);
    assertEquals(25, link.namedNodeIds.get("323.223.24.14").get);
    assertTrue(Integer.valueOf(16).equals(link.anonymousNodeIds(0))
      || Integer.valueOf(71).equals(link.anonymousNodeIds(0)));
  }

  @Test
  def testParseNodeLink() = {
    var parser = new Parser();
    var nl = parser.parse[NodeLink]("nodelink N134:32.23.12.44 : L123");
    assertEquals(123, nl.link);
    assertEquals(134, nl.node);
    assertEquals("32.23.12.44", nl.ip);

    nl = parser.parse("nodelink N12 : L22");
    assertEquals(12, nl.node);
    assertEquals(22, nl.link);
  }
}
