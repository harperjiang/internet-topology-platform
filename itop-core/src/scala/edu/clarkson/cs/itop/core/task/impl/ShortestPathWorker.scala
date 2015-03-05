package edu.clarkson.cs.itop.core.task.impl

import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.task.Task
import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.itop.core.model.Index
import scala.util.control.Breaks._
import edu.clarkson.cs.itop.core.task.TaskParam

/**
 * This worker tries to find a shortest path between the start node and the destination node in the graph.
 * The worker uses DFS to search for possible paths.
 */
class ShortestPathWorker extends TaskWorker {

  var destId = -1;
  var expectedDepth = -1;
  var currentPath: Path = new Path;
  var existedPath: Path = new InfPath;

  override def start(t: Task) = {
    if (t.parent != null) { // Spawned Task, should load information from context
      expectedDepth = TaskParam.getInt(t, "depthRemain")
    }
    currentPath.push(new PathNode(t.context.partition.nodeMap.get(t.startNodeId).get, null, null, null));
  }

  override def spawnTo(t: Task, nodeId: Int, partitionId: Int) = {
    // This is the remaining path length that will be passed to child task
    TaskParam.setInt(t, "depthRemain", expectedDepth - currentPath.length)((partitionId, nodeId));
    // This is the current path to the spawned node
    TaskParam.setObject(t, "path", currentPath)((partitionId, nodeId));
  }

  override def collect(t: Task, fromPartition: Int, nodeId: Int) = {
    // Retrieve path info from subtask
    var subtaskPath = TaskParam.getObject(t, "result", classOf[Path])((fromPartition, nodeId));
    if (subtaskPath != null) {
      // Retrieve stored path
      var localPath = TaskParam.getObject(t, "path", classOf[Path])((fromPartition, nodeId));
      // If subtask is not empty, connect two parts together and store a valid path
      localPath.join(subtaskPath);

      if (localPath.length < existedPath.length) {
        existedPath = localPath;
      }
    }
  }

  override def done(t: Task) = {
    // Child process should store its path in context
    if (existedPath != null) {
      TaskParam.setBoolean(t, "found", true);
      TaskParam.setObject(t, "result", existedPath);
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

      if (expectedDepth > currentPath.length && currentPath.length < existedPath.length) {
        // Go through all child nodes through all the links to find unvisited ones
        var currentPathNode = currentPath.top

        var links = node.links;

        while (links.hasNext()) {
          var link = links.next();
          var nodes = link.nodes;
          while (nodes.hasNext()) {
            var nextChildNode = nodes.next();
            if (!isVisited(t, nextChildNode.id)) {
              var newPathNode = new PathNode(nextChildNode, link, nodes.index, links.index);
              currentPath.push(newPathNode);
              return Some(nextChildNode);
            }
          }
        }
      }
    }
    // No unvisited child, return next sibling. 
    var currentPathNode = currentPath.pop

    var next = nextSibling(currentPathNode);
    if (next == null) {
      if (currentPath.isEmpty)
        // Nothing had been found
        return None;
      return Some(currentPath.top.node);
    } else {
      currentPath.push(next);
      return Some(next.node);
    }

    return None;
  }

  private def nextSibling(current: PathNode): PathNode = {
    var links = currentPath.top.node.links;
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

  var nodes = new java.util.ArrayList[PathNode];

  def length = nodes.size;

  def pop: PathNode = {
    if (nodes.isEmpty)
      return null;
    return nodes.remove(nodes.size - 1)
  };

  def push(node: PathNode): Unit = {
    nodes.add(node);
  }

  def top: PathNode = {
    if (nodes.isEmpty)
      return null;
    return nodes.get(nodes.size - 1)
  }

  def isEmpty: Boolean = nodes.isEmpty

  /**
   * Join two paths together
   */
  def join(another: Path): Unit = {
    if (another.length == 0)
      return ;
    if (length == 0) {
      this.nodes.addAll(another.nodes);
      return ;
    }
    if (another.nodes.get(0).node.id == nodes.get(length - 1).node.id) {
      another.nodes.remove(0);
      this.nodes.addAll(another.nodes);
    }
  }
}

class InfPath extends Path {
  override def length = Integer.MAX_VALUE;
}