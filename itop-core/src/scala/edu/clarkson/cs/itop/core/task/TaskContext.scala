package edu.clarkson.cs.itop.core.task

import edu.clarkson.cs.itop.core.dist.WorkerNode
import edu.clarkson.cs.itop.core.store.KeyValueStore
import edu.clarkson.cs.itop.core.model.Partition

class TaskContext(rootTaskId: (Int, String), wNode: WorkerNode, ptn: Partition, kvstore: KeyValueStore) {

  var workerNode = wNode;
  var partition = ptn;

  def get(key: String): String = {
    return kvstore.get(transform(key))
  }

  def set(key: String, value: String): Unit = {
    kvstore.set(transform(key), value)
  }

  def getObject[T](key: String, clazz: Class[T]): T = {
    return kvstore.getObject[T](transform(key), clazz);
  }

  def setObject(key: String, value: Object) = {
    kvstore.setObject(transform(key), value);
  }

  private def transform(key: String): String = {
    "%d-%s-%s".format(rootTaskId._1, rootTaskId._2, key)
  }

}
