package edu.clarkson.cs.itop.core

import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.jms.core.JmsTemplate
import edu.clarkson.cs.itop.core.dist.message.TaskSubmit
import edu.clarkson.cs.scala.common.message.Sender
import java.util.UUID
import org.slf4j.LoggerFactory
import java.util.Date
import edu.clarkson.cs.itop.core.util.NamingUtils

/**
 * Client Entry
 */
class ClientUnit extends Sender {

  def submitTask(id: String, workerClassName: String, startNodeId: Int, targetPartitionId: Int) = {
    var taskSubmit = new TaskSubmit;
    taskSubmit.taskId = id;
    taskSubmit.workerClassName = workerClassName;
    taskSubmit.startNodeId = startNodeId;
    taskSubmit.targetPartitionId = targetPartitionId;
    send("taskSubmitDest", (taskSubmit, m => { m.setIntProperty("targetPartition", targetPartitionId) }));
  }
}

object RunClient extends App {
  var appContext = new ClassPathXmlApplicationContext("app-context-client.xml");
  var clientUnit = appContext.getBean("clientUnit", classOf[ClientUnit]);
  var logger = LoggerFactory.getLogger(RunClient.getClass);

  args(0) match {
    case "submit" => {
      var workerClassName = args(1)
      var startNodeId = args(2).toInt
      var targetPartitionId = args(3).toInt

      var taskId = NamingUtils.taskId

      clientUnit.submitTask(taskId, workerClassName, startNodeId, targetPartitionId);

      logger.info("Task submitted: %s".format(taskId))
    }
    case _ => {
      // Display help info
      println("Usage: RunClient command <args>");
    }
  }
}