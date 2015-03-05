package edu.clarkson.cs.itop.core.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import org.slf4j.LoggerFactory
import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.itop.core.task.Task
import edu.clarkson.cs.itop.core.task.TaskStatus

class DefaultScheduler extends Scheduler {

  var threadPool: ExecutorService = null;
  private val waitingQueue = new ConcurrentHashMap[(Int, String), Task]();
  private val logger = LoggerFactory.getLogger(getClass());

  /**
   * Schedule a new task
   */
  override def schedule(task: Task): Unit = {
    task.status = TaskStatus.ACTIVE;
    threadPool.submit(new TaskRunner(task, (t: Task, e: Exception) => {
      if (t.spawned != 0) {
        // has unreturned spawned 
        t.status = TaskStatus.WAIT_FOR_SUB;
        waitingQueue.put(t.id, t);
      } else {
        t.status = TaskStatus.END;
        this.onTaskEnd(t, e == null);
      }
    }));
  }

  /**
   * Collect result from spawned tasks
   */
  override def collect(tid: (Int, String), fromPartition: Int, spawnNode: Int): Unit = {
    if (!waitingQueue.containsKey(tid)) {
      logger.warn("Requested task not found:%s".format(tid));
      return
    }
    var task = waitingQueue.get(tid)
    task.synchronized {
      if (task.spawned == 0) {
        return ;
      }
      task.spawned -= 1
      if (task.spawned == 0) {
        waitingQueue.remove(tid);
      }
      val remain = task.spawned;
      threadPool.submit(new CollectRunner(task, fromPartition, spawnNode, (t: Task, e: Exception) => {
        if (remain == 0) {
          t.status = TaskStatus.END;
          this.onTaskEnd(t, t.hasError);
        }
      }));
    };
  }
}