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

    if (!visited.contains(node.id)) {
      visited.add(node.id);
      // Go through all child nodes through all the links to find unvisited ones
      // TODO Check current depth
      var currentPathNode = stack.top

      var links = node.links;

      while (links.hasNext()) {
        var link = links.next();
        var nodes = link.nodes;
        while (nodes.hasNext()) {
          var nextChildNode = nodes.next();
          if (!visited.contains(nextChildNode.id)) {
            var newPathNode = new PathNode(nextChildNode, link, nodes.index, links.index);
            stack.push(newPathNode);
            return Some(nextChildNode);
          }
        }
      }
    }
    // No unvisited child, return next sibling. 
    var currentPathNode = stack.pop

    var next = nextSibling(currentPathNode);
    if (next == null) {
      return Some(stack.top.node);
    } else {
      stack.push(next);
      return Some(next.node);
    }

    return None;
  }

  private def nextSibling(current: PathNode): PathNode = {
    var links = stack.top.node.links;
    links.to(current.linkIndex);
    var nodes = current.link.nodes;
    nodes.to(current.nodeIndex);

    if (nodes.hasNext()) {
      return new PathNode(nodes.next, current.link, nodes.index, current.linkIndex);
    } else if (links.hasNext) {
      var link = links.next;
      var newnodes = link.nodes;
      if (newnodes.hasNext) {
        var node = newnodes.next;
        return new PathNode(node, link, newnodes.index, links.index);
      }
    }
    return null;
  }

  override def collect(t: Task, result: KVStore) = {

  }

  override def done(t: Task) = {

  }

}

class PathNode {
  // Latest node
  var node: Node = null;
  // The link connecting this node and its parent
  var link: Link = null;
  // The link's index in parent node
  var linkIndex: Index = null;
  // The node's index in link
  var nodeIndex: Index = null;
  def this(n: Node, l: Link, ni: Index, li: Index) = {
    this();
    node = n;
    link = l;
    nodeIndex = ni;
    linkIndex = li;
  }
}