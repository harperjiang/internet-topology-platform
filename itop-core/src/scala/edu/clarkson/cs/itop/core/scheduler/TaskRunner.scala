package edu.clarkson.cs.itop.core.scheduler

import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory
import edu.clarkson.cs.itop.core.dist.message.SubtaskExecute
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.task.Task

class TaskRunner(t: Task, cb: (Task, Exception) => Unit) extends Runnable {

  val task: Task = t;

  val callback = cb;

  val logger = LoggerFactory.getLogger(getClass());

  override def run = {
    val worker = task.getWorker;
    val context = task.context;
    val partition = context.partition;
    val com = context.workerNode;
    worker.start(t);

    var exception: Exception = null;

    try {
      var currentNode: Option[Node] = Some(partition.nodeMap.get(task.startNodeId)
        .getOrElse(throw new IllegalArgumentException("No such node:%d".format(task.startNodeId))));
      while (currentNode != None) {
        if (task.parent == null || task.startNodeId != currentNode.get.id) {
          // For subtasks that just started, don't spawn to avoid infinite loop on the spawn point
          var tospawn = partition.queryPartition(currentNode.get);
          if (!tospawn.isEmpty) {
            spawn(task, currentNode.get.id, tospawn)
          }
        }
        currentNode = worker.workon(task, currentNode.get)
      }
    } catch {
      case e: Exception => {
        exception = e;
        logger.warn("Exception in taskRunner", exception);
      }
    }
    task.hasError = (exception != null);

    if (task.spawned == 0) { worker.done(t); }
    callback(task, exception);
  }

  def spawn(task: Task, nid: Int, dests: Iterable[Int]) = {
    // The destination may contain local partition number, need to skip it
    val localId = this.task.context.partition.id;

    dests.filter(_ != localId).foreach(dest => {
      // Create spawning task
      task.getWorker.spawnTo(task, nid, dest);
      var sub = new SubtaskExecute(task, dest, nid);
      task.context.workerNode.sendSubtaskRequest(sub);
      task.spawned += 1;
    });
  }
}