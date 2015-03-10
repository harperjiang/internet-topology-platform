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
      expectedDepth = TaskParam.getInt(t, "depthRemain");
    }
    currentPath.push(new PathNode(t.context.partition.nodeMap.get(t.startNodeId).get, null, null, null));
  }

  override def spawnTo(t: Task, partitionId: Int, nodeId: Int) = {
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
      // Root task should record a found message
      if (t.isRoot)
        TaskParam.setBoolean(t, "found", true);
      TaskParam.setObject(t, "result", existedPath);
    }
    // Nothing to do here cause the valid path should have been stored in collect process
  }

  override def workon(t: Task, node: Node): (Boolean, Option[Node]) = {
    var context = t.context;

    var visited = isNodeVisited(t, node.id);
    if (!visited) {
      setNodeVisited(t, node.id);

      if (node.id == destId) {
        // Is this path shorter than existing path?
        if (existedPath == null || currentPath.length < existedPath.length) {
          existedPath = currentPath.clonePath();
        }
        // This path is done. Which node should we look at next? 
        // We should go to parent's sibling. It doesn't make sense to check 
        // current node (the destination)'s sibling

        if (currentPath.length <= 2) {
          // The parent has no sibling
          return (!visited, None);
        }
        popNode(t);
        popNode(t);
        var grandparent = currentPath.top;
        return (!visited, Some(grandparent.node));
      }

      if (expectedDepth > currentPath.length && currentPath.length < existedPath.length) {
        // Go through all child nodes through all the links to find unvisited ones
        var currentPathNode = currentPath.top

        var links = node.links;

        while (links.hasNext()) {
          var link = links.next();
          if (!isLinkVisited(t, link.id)) {
            var nodes = link.nodes;
            while (nodes.hasNext()) {
              var nextChildNode = nodes.next();
              if (!isNodeVisited(t, nextChildNode.id)) {
                var newPathNode = new PathNode(nextChildNode, link, nodes.index, links.index);
                pushNode(t, newPathNode);
                return (!visited, Some(nextChildNode));
              }
            }
          }
        }
      }
    }
    // No unvisited child, return next sibling. 
    var currentPathNode = popNode(t);

    var next = nextSibling(t, currentPathNode);
    if (next == null) {
      if (currentPath.isEmpty)
        // Nothing had been found
        return (!visited, None);
      return (!visited, Some(currentPath.top.node));
    } else {
      pushNode(t, next);
      return (!visited, Some(next.node));
    }

    return (!visited, None);
  }

  private def nextSibling(t: Task, current: PathNode): PathNode = {
    if (currentPath.isEmpty)
      return null;
    var links = currentPath.top.node.links;
    var link = links.to(current.linkIndex);
    var nodes = current.link.nodes;
    nodes.to(current.nodeIndex);

    while (link != null) {
      while (nodes.hasNext()) {
        var node = nodes.next();
        if (!isNodeVisited(t, node.id)) {
          return new PathNode(node, link, nodes.index, links.index);
        }
      }
      if (links.hasNext())
        link = links.next;
      else
        link = null;
    }
    return null;
  }

  private def isNodeVisited(t: Task, nodeId: Int): Boolean = {
    return "true".equals(t.context.get("nodeVisited.%d".format(nodeId)));
  }

  private def setNodeVisited(t: Task, nodeId: Int) = {
    t.context.set("nodeVisited.%d".format(nodeId), "true")
  }

  private def isLinkVisited(t: Task, linkId: Int): Boolean = {
    return "true".equals(t.context.get("linkVisited.%d".format(linkId)));
  }

  private def setLinkVisited(t: Task, linkId: Int, visited: Boolean) = {
    t.context.set("linkVisited.%d".format(linkId), visited.toString);
  }

  private def pushNode(t: Task, pn: PathNode) = {
    currentPath.push(pn);
    if (pn.link != null) {
      setLinkVisited(t, pn.link.id, true);
    }
  }

  private def popNode(t: Task): PathNode = {
    if (currentPath.isEmpty)
      return null;
    var pn = currentPath.pop;
    if (pn.link != null) {
      setLinkVisited(t, pn.link.id, false);
    }
    return pn;
  }
}

