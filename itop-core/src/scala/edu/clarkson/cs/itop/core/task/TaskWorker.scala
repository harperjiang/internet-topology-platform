package edu.clarkson.cs.itop.core.task

import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.scala.common.message.KVStore

/**
 * <code>TaskWorker</code> is the interface provided to users who want to
 * implement their own vertex programs.
 */
trait TaskWorker {

  /**
   * Callback when the worker is to be started
   */
  def start(t: Task);

  /**
   * Work on the current node.
   * Return The next node it wants to execute on.
   * If no more node to work on, return None
   */
  def workon(t: Task, node: Node): (Boolean, Option[Node]);

  /**
   * Notify the worker that a spawned task has been generated
   */
  def spawnTo(t: Task, nodeId: Int, partitionId: Int): Unit;

  /**
   * Collect result from spawned processes. Data could be retrieved from TaskContext
   */
  def collect(t: Task, fromPartition: Int, nodeId: Int);

  /**
   * The output of this TaskWorker
   */
  def summary(t: Task): KVStore = { return null }

  /**
   * Callback when the task is done
   */
  def done(t: Task);

}