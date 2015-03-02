package edu.clarkson.cs.itop.core.task.impl

import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.task.Task
import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.itop.core.model.Index
import scala.util.control.Breaks._

/**
 * This worker tries to find a shortest path between the start node and the destination node in the graph.
 * The worker uses DFS to search for possible paths.
 */
class ShortestPathWorker extends TaskWorker {

  var destId = -1
  var expectedDepth = -1
  var stack = scala.collection.mutable.Stack[PathNode]();
  var visited = scala.collection.mutable.Set[Integer]();

  override def start(t: Task) = {}

  override def workon(t: Task, node: Node): Option[Node] = {

    var context = t.context;

    if (node.id == destId) {
      // Found
      return None;
    }

    // First go for next unvisited child in current node's list
    var currentPathNode = stack.top

    if (currentPathNode.node == null) {
      var lastLink = currentPathNode.link;
      var current = lastLink.nodes.first;
      var lastIndex = lastLink.nodes.last._2;

      // Go through all child nodes to find unvisited ones
      // TODO Check current depth
      
      while (current._2 != lastIndex) {
        var node = current._1;
        if (!visited.contains(node.id)) {
          // This is the next node to be visited
          var newPathNode = new PathNode(node, current._2);
          stack.push(newPathNode);
          return Some(node);
        }
        current = lastLink.nodes.next(current._2);
      }
      // No child unvisited, should go to next sibling. 
      var oldPathNode = stack.pop
      
    } else {

    }

    return None;
  }

  override def collect(t: Task, result: KVStore) = {

  }

  override def done(t: Task) = {

  }

}

class PathNode {

  var node: Node = null;
  var link: Link = null;
  var index: Index = null;

  def this(n: Node, i: Index) = {
    this();
    node = n;
    index = i;
  }

  def this(l: Link, i: Index) = {
    this();
    link = l;
    index = i;
  }
}