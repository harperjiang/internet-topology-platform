package edu.clarkson.cs.itop.core

import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.jms.core.JmsTemplate
import edu.clarkson.cs.itop.core.dist.message.TaskSubmit
import edu.clarkson.cs.scala.common.message.Sender

/**
 * Client Entry
 */
class ClientUnit extends Sender {

  def submitTask(workerClassName: String, startNodeId: Int, targetPartitionId: Int) = {
    var taskSubmit = new TaskSubmit;
    taskSubmit.workerClassName = workerClassName;
    taskSubmit.startNodeId = startNodeId;
    taskSubmit.targetPartitionId = targetPartitionId;
    send("taskSubmitDest", (taskSubmit, m => { m.setIntProperty("targetPartition", targetPartitionId) }));
  }
}

object RunClient extends App {
  var appContext = new ClassPathXmlApplicationContext("app-context-client.xml");
  var clientUnit = appContext.getBean("clientUnit", classOf[ClientUnit]);

  args(0) match {
    case "submit" => {
      var workerClassName = args(1)
      var startNodeId = args(2).toInt
      var targetPartitionId = args(3).toInt
      clientUnit.submitTask(workerClassName, startNodeId, targetPartitionId);
    }
    case _ => {
      // Display help info
      println("Usage: RunClient command <args>");
    }
  }
}