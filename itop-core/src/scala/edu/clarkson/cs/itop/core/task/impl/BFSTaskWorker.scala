package edu.clarkson.cs.itop.core.task.impl

import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.task.Task
import scala.collection.mutable.ArrayBuffer

abstract class BFSTaskWorker extends AbstractTaskWorker {

  protected var buffer = new ArrayBuffer[Node]();

  override def start(t: Task) = {
    buffer += t.context.partition.nodeMap.get(t.startNodeId).get;
  }

  override def workon(t: Task, node: Node): (Boolean, Option[Node]) = {
    while (isNodeVisited(t, buffer.head.id)) {
      buffer.remove(0);
    }
    if (buffer.isEmpty) {
      return (false, None);
    }
    // Add all children to buffer
    var currentNode = buffer.head;

    var links = currentNode.links;
    while (links.hasNext()) {
      var link = links.next();
      var nodes = link.nodes;
      while (nodes.hasNext()) {
        var node = nodes.next;
        if (!isNodeVisited(t, node.id)) {
          buffer += node;
        }
      }
    }
    process(buffer.head);
    buffer.remove(0);
    return (true, Some(buffer.head));
  }

  def process(node: Node): Unit;
}