package edu.clarkson.cs.itop.core.task.impl

import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.itop.core.task.Task

abstract class AbstractTaskWorker extends TaskWorker {

  protected def isNodeVisited(t: Task, nodeId: Int): Boolean = {
    return "true".equals(t.context.get("nodeVisited.%d".format(nodeId)));
  }

  protected def setNodeVisited(t: Task, nodeId: Int) = {
    t.context.set("nodeVisited.%d".format(nodeId), "true")
  }

  protected def isLinkVisited(t: Task, linkId: Int): Boolean = {
    return "true".equals(t.context.get("linkVisited.%d".format(linkId)));
  }

  protected def setLinkVisited(t: Task, linkId: Int, visited: Boolean) = {
    t.context.set("linkVisited.%d".format(linkId), visited.toString);
  }
}