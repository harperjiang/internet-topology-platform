package edu.clarkson.cs.itop.core.scheduler

import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.itop.core.task.Task

class CollectRunner(origin: Task, r: KVStore, cb: (Task, Exception) => Unit) extends Runnable {

  val task = origin;
  val callback = cb;
  val result = r;

  def run = {
    var exception: Exception = null;

    try {
      task.getWorker.collect(task, result);
    } catch {
      case e: Exception => {
        exception = e;
      }
    }
    task.hasError |= (exception != null);

    if (task.spawned == 0)
      task.getWorker.done(task);
    callback(task, exception);
  }
}