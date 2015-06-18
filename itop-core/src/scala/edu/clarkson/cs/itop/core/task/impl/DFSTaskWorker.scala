package edu.clarkson.cs.itop.core.task.impl

import edu.clarkson.cs.itop.core.model.Node
import scala.collection.mutable.ArrayBuffer
import edu.clarkson.cs.itop.core.task.Task
import edu.clarkson.cs.itop.core.task.TaskParam

abstract class DFSTaskWorker extends AbstractTaskWorker {

  var currentPath: Path = new Path;
  var existedPath: Path = null;
  var expectedDepth = 0;

  override def start(t: Task) = {
    if (t.parent != null) { // Spawned Task, should load information from context
      expectedDepth = TaskParam.getInt(t, "depthRemain");
    }
    currentPath.push(new PathNode(t.context.partition.nodeMap.get(t.startNodeId).get, null, null, null));
  }

  override def spawnTo(t: Task, partitionId: Int, nodeId: Int) = {
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
      process(node);
      setNodeVisited(t, node.id);

      if (test(node)) { // Meet condition
        // Save current path
        var continue = savePath(currentPath);

        if (!continue) { // Should stop
          return (!visited, None);
        } else {
          if (currentPath.length <= 2) {
            // The parent has no sibling
            return (!visited, None);
          }
          popNode(t);
          popNode(t);
          var grandparent = currentPath.top;
          return (!visited, Some(grandparent.node));
        }
      }

      if (checkPath(currentPath)) {
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

  /**
   * Test whether the target has been found
   *
   * Return : true if target has been found
   *          false if not
   */
  def test(node: Node): Boolean;

  /**
   * Return : continue
   *            true if continue the search
   *            false if should stop
   */
  def savePath(path: Path): Boolean;

  /**
   * Return : continue
   *            true if continue on this path
   *            false if should abandon
   */
  def checkPath(path: Path): Boolean;

  def process(node: Node): Unit;
}