package edu.clarkson.cs.itop.core.scheduler

import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.task.Task

class LocalWorker extends TaskWorker {

  def start(t: Task) = {

  }
  /**
   * Work on the current node.
   * Return The next node(s) it wants to execute on
   */
  def workon(t: Task, node: Node): Option[Node] = {
    return None;
  }

  def collect(t: Task, fromPartition: Int, nodeId: Int, result: KVStore) = {

  }

  def spawnTo(t: Task, nodeId: Int, partitionId: Int) = {

  }
  /**
   * Callback when the task is done
   */
  def done(t: Task) = {

  }

}