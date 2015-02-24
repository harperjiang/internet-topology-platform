package edu.clarkson.cs.itop.core.task

import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.itop.core.model.Node

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
   * Return The next node(s) it wants to execute on
   */
  def execute(t: Task, node: Node): Iterable[Node];

  /**
   * Collect result from spawned processes
   */
  def collect(t: Task, result: KVStore);

  /**
   * Callback when the task is done
   */
  def done(t: Task);

  /**
   * Data return from this method will be collected with JMS collector
   */
  def export: KVStore = { return null }
}