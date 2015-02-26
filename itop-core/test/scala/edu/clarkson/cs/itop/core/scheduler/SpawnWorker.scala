package edu.clarkson.cs.itop.core.scheduler

import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.task.Task


class SpawnWorker extends TaskWorker {

  def start(t: Task) = {

  }
  /**
   * Work on the current node.
   * Return The next node(s) it wants to execute on
   */
  def workon(t: Task, node: Node): Option[Node] = {
    return None;
  }

  /**
   * Collect result from spawned processes
   */
  def collect(t: Task, result: KVStore) = {

  }

  /**
   * Callback when the task is done
   */
  def done(t: Task) = {

  }
}