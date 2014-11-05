package edu.clarkson.cs.itop.core.model

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer

/**
 * A link consists of multiple nodes and it works like a shared bus between these links.
 * Each node may have a IP dedicated to that link.
 * This means that if this IP appears, the traffic for sure goes to this link.
 */
class Link(lid: Int) {

  var id = lid;

  val namedNodeIds = scala.collection.mutable.Map[String, Int]();
  val anonymousNodeIds = new ArrayBuffer[Int];

  val namedNodes = scala.collection.mutable.Map[String, Node]();
  val anonymousNodes = new ArrayBuffer[Node]();

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

    namedNodeIds.foreach(entry => {
      namedNodes += (entry._1 -> nodeMap.get(entry._2).getOrElse(nodeNotFound(entry._2)));
    });
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

}