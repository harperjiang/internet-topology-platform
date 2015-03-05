package edu.clarkson.cs.itop.core.scheduler

import java.util.EventListener
import java.util.EventObject
import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.scala.common.EventListenerSupport
import edu.clarkson.cs.itop.core.task.Task

trait Scheduler extends EventListenerSupport[SchedulerListener] {

  def schedule(task: Task);

  def collect(taskId: (Int, String), fromPartition: Int, fromNode: Int);

  protected def onTaskEnd(task: Task, success: Boolean) {
    val e = new SchedulerEvent(this, task, success);
    listeners.foreach(l => l.onTaskEnd(e));
  }
}

trait SchedulerListener extends EventListener {
  def onTaskEnd(event: SchedulerEvent);
}

class SchedulerEvent(scheduler: Scheduler, t: Task, suc: Boolean)
    extends EventObject(scheduler) {
  val success = suc;
  val task = t;
}