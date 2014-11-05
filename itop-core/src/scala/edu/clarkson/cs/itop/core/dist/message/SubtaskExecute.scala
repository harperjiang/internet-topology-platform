package edu.clarkson.cs.itop.core.dist.message

import scala.beans.BeanProperty
import edu.clarkson.cs.itop.core.mapreduce.Task
import edu.clarkson.cs.itop.core.mapreduce.Task
import edu.clarkson.cs.itop.core.task.Task

class SubtaskExecute {
  @BeanProperty
  var parentMachine = 0;
  @BeanProperty
  var parentTaskId = "";
  @BeanProperty
  var workerClass = "";
  @BeanProperty
  var targetPartition = 0;
  @BeanProperty
  var targetNodeId = 0;

  def this(parent: Task, tpid: Int, tnid: Int) = {
    this();
    this.parentMachine = parent.id._1;
    this.parentTaskId = parent.id._2;
    this.workerClass = parent.workerClass.getName();
    this.targetNodeId = tnid;
    this.targetPartition = tpid;
  }

  def parentId = (parentMachine, parentTaskId);
}