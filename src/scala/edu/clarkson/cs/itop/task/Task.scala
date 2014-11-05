package edu.clarkson.cs.itop.task

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer

/**
 * A Task is a vertex program
 */

object TaskStatus extends Enumeration {
  type T = Value
  val READY, ACTIVE, WAIT_FOR_SUB, END = Value
};

class Task {

  /**
   * Task identification
   */
  var id: (Int, String) = null;

  /**
   * Subtask only, parent id
   */
  var parent: (Int, String) = null;

  /**
   * Task Status
   */
  var status: TaskStatus.T = TaskStatus.READY;

  /**
   * The start node of this task
   */
  var startNodeId: Int = 0;

  /**
   * How many items has been spawned from it
   */
  var spawned: Int = 0;

  /**
   * Has error in execution
   */
  var hasError: Boolean = false;

  /**
   * Worker for task execution
   */
  var workerClass: Class[_ <: TaskWorker] = null;

  /**
   * Task Context
   */
  var context: TaskContext = null;

  @transient
  private var worker: Option[TaskWorker] = None;

  def this(pid: (Int, String)) = {
    this();
    this.parent = pid;
  }

  def getWorker: TaskWorker = {
    worker.getOrElse({ worker = Some(workerClass.newInstance()); worker.get; });
  }
}
