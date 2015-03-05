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
  var currentPath: Path = null;
  var existedPath: Path = null;

  override def start(t: Task) = {
    if (t.parent != null) { // Spawned Task, should load information from context
      expectedDepth = t.context.get("%d-%d.depthRemain".format(t.context.partition.id, t.startNodeId)).toInt;
    }
    stack.push(new PathNode(t.context.partition.nodeMap.get(t.startNodeId).get, null, null, null));
  }

  override def spawnTo(t: Task, nodeId: Int, partitionId: Int) = {
    // This is the remaining path length that will be passed to child task
    t.context.set("%d-%d.depthRemain".format(partitionId, nodeId), (expectedDepth - stack.size).toString)
    // This is the current path to the spawned node
    t.context.setObject("%d-%d.path".format(partitionId, nodeId), stack)
  }

  override def collect(t: Task, fromPartition: Int, nodeId: Int) = {
    // Retrieve path info from subtask
    var subtaskPath = t.context.get("%d-%d.result");
    if (subtaskPath != null) {
      // Retrieve stored path
      var localPath = t.context.get("%d-%d.path");
      // If subtask is not empty, connect two parts together and store a valid path

    }
  }

  override def done(t: Task) = {
    // Child process should store its path in context
    if (existedPath != null) {
      t.context.set("%d-%d.found".format(t.context.partition.id, t.startNodeId), "true");
      t.context.setObject("%d-%d.result".format(t.context.partition.id, t.startNodeId), existedPath)
    }
    // Nothing to do here cause the valid path should have been stored in collect process
  }

  override def workon(t: Task, node: Node): Option[Node] = {
    var context = t.context;

    if (!isVisited(t, node.id)) {
      setVisited(t, node.id);

      if (node.id == destId) {
        // Is this path shorter than existing path?
        if (existedPath == null || currentPath.length < existedPath.length) {
          existedPath = currentPath;
        }
        // This path is done. Which node should we look at next? 
        // We should go to parent's sibling. It doesn't make sense to check 
        // current node (the destination)'s sibling

        if (currentPath.length <= 2) {
          // The parent has no sibling
          return None;
        }
        currentPath.pop
        var parent = currentPath.pop
        var parentSibling = nextSibling(parent)
        return Some(parentSibling.node);
      }

      if (expectedDepth > stack.size && currentPath.length < existedPath.length) {
        // Go through all child nodes through all the links to find unvisited ones
        var currentPathNode = stack.top

        var links = node.links;

        while (links.hasNext()) {
          var link = links.next();
          var nodes = link.nodes;
          while (nodes.hasNext()) {
            var nextChildNode = nodes.next();
            if (!isVisited(t, nextChildNode.id)) {
              var newPathNode = new PathNode(nextChildNode, link, nodes.index, links.index);
              stack.push(newPathNode);
              return Some(nextChildNode);
            }
          }
        }
      }
    }
    // No unvisited child, return next sibling. 
    var currentPathNode = stack.pop

    var next = nextSibling(currentPathNode);
    if (next == null) {
      if (stack.isEmpty)
        // Nothing had been found
        return None;
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

  private def isVisited(t: Task, nodeId: Int): Boolean = {
    return "true".equals(t.context.get("visited.%d".format(nodeId)));
  }

  private def setVisited(t: Task, nodeId: Int) = {
    t.context.set("visited.%d".format(nodeId), "true")
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

class Path {

  def length = 0;

  def pop: PathNode = null;
}