package edu.clarkson.cs.itop.core.model

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

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
  var anonymousNodes = new ArrayBuffer[Node]();

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

  def attachNodes(nodeMap: scala.collection.mutable.Map[Int, Node]) = {
    var nodeNotFound = { node_id: Int => { throw new IllegalArgumentException("Node not found: %d".format(node_id)); } };

    var nodeMaps = namedNodeIds.mapValues(node_id => nodeMap.get(node_id).getOrElse(nodeNotFound(node_id)));
    namedNodes = TreeMap(nodeMaps.toArray: _*);
    anonymousNodeIds.foreach(entry => {
      anonymousNodes += nodeMap.get(entry).getOrElse(nodeNotFound(entry));
    });

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

  def foreachNode(f: ((Node, NodeIndex)) => Unit) = {
    var index = new NodeIndex();
    index.onNamedNodes = true;
    index.nameKey = namedNodes.firstKey;
    var current = nodeAtIndex(index);
    while (current != null) {
      f(current);
      current = nextNode(current._2);
    }
  }

  def nodeAtIndex(index: NodeIndex): (Node, NodeIndex) = {
    var node: Node = null;
    if (index.onNamedNodes) {
      node = namedNodes.get(index.nameKey).get;
    } else if (index.anonymousIndex < anonymousNodes.length) {
      node = anonymousNodes(index.anonymousIndex);
    }
    if (node == null)
      return null;
    return (node, index);
  }

  def nextNode(index: NodeIndex): (Node, NodeIndex) = {
    // Name nodes first, anonymous nodes then
    var newIndex = new NodeIndex(index);

    if (index.onNamedNodes) {
      var iterator = namedNodes.iteratorFrom(index.nameKey);
      // Ignore this one and return the "next" one
      iterator.next
      var expect = iterator.next;
      if (expect == null) {
        // No more named nodes, should switch to anonymous nodes
        newIndex.onNamedNodes = false;
        newIndex.anonymousIndex = 0;
      } else {
        newIndex.nameKey = expect._1;
      }
    } else {
      newIndex.anonymousIndex += 1;
    }
    return nodeAtIndex(newIndex);
  }
}

/**
 * NodeIndex uniquely identify a node attached to a link
 */
class NodeIndex {
  var onNamedNodes = false;
  var nameKey = "";
  var anonymousIndex = 0;

  def this(copy: NodeIndex) = {
    this();
    this.onNamedNodes = copy.onNamedNodes;
    this.nameKey = copy.nameKey;
    this.anonymousIndex = copy.anonymousIndex;
  }
}