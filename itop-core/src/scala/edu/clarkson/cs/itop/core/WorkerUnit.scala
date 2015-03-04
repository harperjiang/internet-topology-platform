package edu.clarkson.cs.itop.core

import java.util.UUID
import org.springframework.beans.factory.InitializingBean
import org.springframework.context.support.ClassPathXmlApplicationContext
import edu.clarkson.cs.itop.core.task.TaskContext
import edu.clarkson.cs.itop.core.dist.WorkerListener
import edu.clarkson.cs.itop.core.scheduler.SchedulerEvent
import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.itop.core.dist.message.SubtaskExecute
import edu.clarkson.cs.itop.core.dist.message.SubtaskResult
import edu.clarkson.cs.itop.core.dist.WorkerNode
import edu.clarkson.cs.itop.core.scheduler.SchedulerListener
import edu.clarkson.cs.itop.core.scheduler.Scheduler
import edu.clarkson.cs.itop.core.model.Partition
import edu.clarkson.cs.itop.core.task.Task
import org.slf4j.LoggerFactory
import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.itop.core.dist.message.TaskSubmit
import edu.clarkson.cs.itop.core.util.NamingUtils
import edu.clarkson.cs.itop.core.store.KeyValueStore

class WorkerUnit extends WorkerListener with SchedulerListener with InitializingBean {

  var node: WorkerNode = null;
  var partition: Partition = null;
  var scheduler: Scheduler = null;
  var kvstore: KeyValueStore = null;

  def afterPropertiesSet() = {
    node.addListener(this);
    scheduler.addListener(this);
  }

  def submit(task: Task): Unit = {
    // Assign valid task id
    if (null == task.id) {
      task.id = taskId
    }
    // Assign task root id
    if (null == task.parent) {
      task.root = task.id;
    }
    // Check task information
    if (task.startNodeId == -1) {
      throw new IllegalArgumentException("Task startNodeId is missing");
    }
    if (task.workerClass == null) {
      throw new IllegalArgumentException("Task workerClass is missing");
    }

    // Submit the task into scheduler
    var ctx = new TaskContext(task.root, node, partition, kvstore);
    task.context = ctx;
    scheduler.schedule(task);
  }

  def submit(expectTaskId: String, workerClassName: String, startNode: Int): Unit = {
    var task = new Task;
    task.id = taskId(expectTaskId);
    task.workerClass = Class.forName(workerClassName).asSubclass(classOf[TaskWorker]);
    task.startNodeId = startNode;
    submit(task);
  }

  /**
   *  WorkerNode Listeners
   */
  override def onRequestReceived(stask: SubtaskExecute) = {
    // submit the subtask to schedule
    var subtask = new Task(stask.parentId, stask.rootId);
    subtask.workerClass = Class.forName(stask.workerClass).asInstanceOf[Class[TaskWorker]];
    subtask.startNodeId = stask.targetNodeId;

    submit(subtask);
  }

  override def onResponseReceived(subtask: SubtaskResult) = {
    // Send the result to scheduler
    scheduler.collect(subtask.parentId, subtask.sourcePartitionId, subtask.sourceFromNodeId, subtask.result);
  }

  override def onTaskSubmitted(task: TaskSubmit) = {
    // Submit the task
    submit(task.taskId, task.workerClassName, task.startNodeId);
  }

  /**
   * Scheduler Listeners
   */
  def onTaskEnd(e: SchedulerEvent) = {
    // On the completion of task
    // If this is a subtask, return it to original caller
    e.task.parent match {
      case pid if (pid != null) => {
        // Non-empty parent, subtask, should be returned to original partition
        var resp = new SubtaskResult(pid, partition.id, e.task.startNodeId, e.task.getWorker.summary(e.task));
        node.sendSubtaskResponse(resp);
      }
      case _ => {
        // Normal task, query to see whether need to collect result using jms collector
        var result = e.task.getWorker.summary(e.task);
        if (null != result) {
          this.node.exportTaskResult(e.task, result);
        }
      }
    }
  }

  def taskId: (Int, String) = {
    (node.machineId, NamingUtils.taskId)
  }

  def taskId(expect: String): (Int, String) = {
    (node.machineId, expect)
  }
}

object RunWorker extends App {
  var logger = LoggerFactory.getLogger(RunWorker.getClass)
  logger.info("Worker Unit Started")
  var appContext = new ClassPathXmlApplicationContext("app-context-worker.xml");
}