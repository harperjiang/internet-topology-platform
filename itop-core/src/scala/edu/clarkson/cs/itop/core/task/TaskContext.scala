package edu.clarkson.cs.itop.core.task

import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.itop.core.dist.WorkerNode
import edu.clarkson.cs.itop.core.model.Partition


class TaskContext(w: WorkerNode, part: Partition) {

  val worker = w;
  val partition = part;
  val result = new KVStore;
}