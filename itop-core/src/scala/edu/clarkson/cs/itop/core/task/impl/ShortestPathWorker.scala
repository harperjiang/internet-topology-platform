package edu.clarkson.cs.itop.core.task.impl

import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.task.Task
import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.scala.common.message.KVStore

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

    // First go for next unvisited child in parent's list
    var currentPos = stack.top
//    node.foreachLink()

    return None;
  }

  override def collect(t: Task, result: KVStore) = {

  }

  override def done(t: Task) = {

  }

}

class PathNode {

  var node: Node = null;
  var nodeIndex = -1;
  var link: Link = null;
  var linkString = "";
  var linkIndex = -1;

}