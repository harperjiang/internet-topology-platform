package edu.clarkson.cs.itop.core.task.impl

import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.task.Task
import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.scala.common.message.KVStore

/**
 * This worker tries to find a shortest path between the start node and the destination node in the graph.
 */
class ShortestPathWorker extends TaskWorker {

  var destId = -1

  override def start(t: Task) = {}

  override def workon(t: Task, node: Node): Option[Node] = {

    if (node.id == destId) {
      // Found
      return None;
    }

    node.anonymousLinks.foreach(link => {

    });

    node.namedLinks.foreach(link => {

    });

    return None;
  }

  override def collect(t: Task, result: KVStore) = {

  }

  override def done(t: Task) = {

  }

}