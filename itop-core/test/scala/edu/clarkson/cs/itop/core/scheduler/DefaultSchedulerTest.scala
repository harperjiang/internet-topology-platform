package edu.clarkson.cs.itop.core.scheduler

import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration

import javax.jms.Message

import edu.clarkson.cs.itop.core.model.routing.DefaultRouting
import edu.clarkson.cs.itop.core.task.TaskStatus
import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.itop.core.dist.message.SubtaskResult
import edu.clarkson.cs.itop.core.dist.WorkerNode
import edu.clarkson.cs.itop.core.model.Partition
import edu.clarkson.cs.itop.core.task.Task
import edu.clarkson.cs.itop.core.dist.message.SubtaskExecute
import edu.clarkson.cs.itop.core.task.TaskContext
import edu.clarkson.cs.itop.core.dist.WorkerListener

class DefaultSchedulerTest {

  var scheduler: DefaultScheduler = null;

  var partition: Partition = null;

  var workerNode: WorkerNode = null;

  @Before
  def init = {
    scheduler = new DefaultScheduler();
    scheduler.threadPool = Executors.newFixedThreadPool(2);

    partition = new Partition();
    partition.nodeFile = "testdata/nodes";
    partition.linkFile = "testdata/links";
    partition.id = 1;

    var routing = new DefaultRouting();
    routing.routingFile = "testdata/routing";
    partition.routing = routing;
    routing.afterPropertiesSet;
    partition.afterPropertiesSet;

    workerNode = new WorkerNode {
      override def send(dest: String, message: (Object, Message => Unit)) = {
        // Do nothing for message sending
      }
    }
    workerNode.machineId = 1;
    workerNode.groupId = 1;
    workerNode.hbInterval = 1000;
    workerNode.afterPropertiesSet;
  }

  @Test
  def testLocalTask = {
    var semaphore = new Semaphore(0);
    var received: Task = null;
    var slistener = new SchedulerListener() {
      def onTaskEnd(event: SchedulerEvent) = {
        received = event.task;
        semaphore.release();
      }
    };
    scheduler.addListener(slistener)

    var task = new Task(null, null);
    task.startNodeId = 1;
    task.workerClass = classOf[LocalWorker];
    task.context = new TaskContext(task.id, workerNode, partition, null);

    scheduler.schedule(task);
    semaphore.acquire();

    assertEquals(received.id, task.id);
    assertEquals(TaskStatus.END, received.status);
  }

  @Test
  def testSpawnedTask = {
    var semaphore = new Semaphore(0);
    var received: Task = null;
    var slistener = new SchedulerListener() {
      def onTaskEnd(event: SchedulerEvent) = {
        received = event.task;
        semaphore.release();
      }
    };
    scheduler.addListener(slistener)

    var task = new Task(null, null);
    task.id = (1, "taskid");
    task.startNodeId = 7;
    task.workerClass = classOf[LocalWorker];
    task.context = new TaskContext(task.id, workerNode, partition, null);

    var targetPartition = 0;
    var targetNodeId = 0;

    workerNode.addListener(new WorkerListener() {
      override def onRequestSent(task: SubtaskExecute) = {
        targetPartition = task.targetPartition;
        targetNodeId = task.targetNodeId;
        semaphore.release();
      }

      override def onResponseReceived(task: SubtaskResult) = {
        scheduler.collect(task.parentId, task.sourcePartitionId, task.sourceFromNodeId, task.result);
        semaphore.release();
      }
    });

    scheduler.schedule(task);
    semaphore.acquire();

    Thread.sleep(1000);
    // Now the task should be suspended for subtask return
    assertEquals(2, targetPartition);
    assertEquals(7, targetNodeId);
    assertEquals(TaskStatus.WAIT_FOR_SUB, task.status);

    var subresult = new SubtaskResult((1, "taskid"), targetPartition, 2, new KVStore());

    workerNode.onResponseReceived(subresult);

    semaphore.acquire(2);

    assertEquals(received.id, task.id);
    assertEquals(TaskStatus.END, received.status);
  }
}