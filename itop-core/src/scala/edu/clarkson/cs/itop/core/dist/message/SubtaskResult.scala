package edu.clarkson.cs.itop.core.dist.message

import scala.beans.BeanProperty
import edu.clarkson.cs.scala.common.message.KVStore

class SubtaskResult {

  @BeanProperty
  var parentMachine = 0;

  @BeanProperty
  var parentTaskId = "";

  @BeanProperty
  var sourcePartitionId: Int = 0;

  /**
   * Indicating the start node of this subtask
   */
  @BeanProperty
  var sourceFromNodeId: Int = 0;

  def this(pid: (Int, String), spid: Int, snid: Int) = {
    this();
    this.parentMachine = pid._1;
    this.parentTaskId = pid._2;
    this.sourcePartitionId = spid;
    this.sourceFromNodeId = snid;
  }
  def parentId = (parentMachine, parentTaskId);

}