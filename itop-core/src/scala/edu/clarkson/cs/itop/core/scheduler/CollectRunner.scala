package edu.clarkson.cs.itop.core.scheduler

import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.itop.core.task.Task

class CollectRunner(origin: Task, fromPartition: Int, fromNode: Int,
    result: KVStore, callback: (Task, Exception) => Unit) extends Runnable {

  def run = {
    var exception: Exception = null;

    try {
      origin.getWorker.collect(origin, fromPartition, fromNode, result);
    } catch {
      case e: Exception => {
        exception = e;
      }
    }
    origin.hasError |= (exception != null);

    if (origin.spawned == 0) {
      origin.getWorker.done(origin);
    }
    callback(origin, exception);
  }
}