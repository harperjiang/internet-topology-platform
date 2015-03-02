package edu.clarkson.cs.itop.core.model

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.SortedMap
import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

/**
 * A link consists of multiple nodes and it works like a shared bus between these links.
 * Each node may have a IP dedicated to that link.
 * This means that if this IP appears, the traffic for sure goes to this link.
 */
class Link(lid: Int) {

  var id = lid;

  val namedNodeIds = scala.collection.mutable.Map[String, Int]();
  val anonymousNodeIds = new ArrayBuffer[Int];

  var namedNodes = SortedMap[String, Node]();
  var anonymousNodes = List[Node]();

  var nodesIterator: IndexableIterator[Node] = null;

  def this(sid: String, nodes: java.util.List[(String, String)]) = {
    this(Integer.parseInt(sid.substring(1)));

    nodes.foreach(a => {
      var id = a._1.substring(1).toInt;
      var ip = a._2;

      if (ip == "") {
        anonymousNodeIds += id;
      } else {
        // Test 
        if (namedNodeIds.contains(ip)) {
          throw new RuntimeException("Duplicate IP for different nodes")
        }
        namedNodeIds += { ip -> id };
      }
    });
  }

  def attachNodes(nodeMap: Map[Int, Node]) = {
    var nodeNotFound = { node_id: Int => { throw new IllegalArgumentException("Node not found: %d".format(node_id)); } };

    var nodeMaps = namedNodeIds.mapValues(node_id => nodeMap.get(node_id).getOrElse(nodeNotFound(node_id)));
    namedNodes = TreeMap(nodeMaps.toArray: _*);
    anonymousNodes = anonymousNodeIds.map(entry => nodeMap.get(entry).getOrElse(nodeNotFound(entry))).toList;

    // Clear data no longer needed
    namedNodeIds.clear;
    anonymousNodeIds.clear;
  }

  def nodeSize = {
    if (namedNodes.isEmpty && anonymousNodes.isEmpty)
      namedNodeIds.size + anonymousNodeIds.size;
    else
      namedNodes.size + anonymousNodes.length;
  }

  def nodes: IndexableIterator[Node] = {
    if (nodesIterator == null) {
      nodesIterator = new IndexableIterator[Node](namedNodes, anonymousNodes);
    }
    return nodesIterator;
  }
}
