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

  def setObject[T](key: String, value: T) = {
    kvstore.setObject(transform(key), value);
  }

  private def transform(key: String): String = {
    "%d-%s-%s".format(rootTaskId._1, rootTaskId._2, key)
  }

}

/**
 * Method for sharing parameters between main tasks and spawned tasks
 */
object TaskParam {

  def getString(t: Task, key: String)(implicit id: (Int, Int) = null): String = {
    t.context.get(makekey(t, id, key))
  }

  def setString(t: Task, key: String, value: String)(implicit id: (Int, Int) = null): Unit = {
    t.context.set(makekey(t, id, key), value);
  }

  def getInt(t: Task, key: String)(implicit id: (Int, Int) = null): Int = {
    t.context.get(makekey(t, id, key)).toInt
  }

  def setInt(t: Task, key: String, value: Int)(implicit id: (Int, Int) = null): Unit = {
    t.context.set(makekey(t, id, key), value.toString);
  }

  def getBoolean(t: Task, key: String)(implicit id: (Int, Int) = null): Boolean = {
    t.context.get(makekey(t, id, key)).toBoolean
  }

  def setBoolean(t: Task, key: String, value: Boolean)(implicit id: (Int, Int) = null): Unit = {
    t.context.set(makekey(t, id, key), value.toString);
  }

  def getObject[T](t: Task, key: String, clazz: Class[T])(implicit id: (Int, Int) = null): T = {
    t.context.getObject(makekey(t, id, key), clazz)
  }

  def setObject[T](t: Task, key: String, value: T)(implicit id: (Int, Int) = null): Unit = {
    t.context.setObject(makekey(t, id, key), value);
  }

  def makekey(t: Task, id: (Int, Int), key: String): String = {
    var realkey: String = null;
    if (id != null) {
      realkey = makekey(t, key);
    } else {
      realkey = makekey(id, key);
    }
    return realkey;
  }

  def makekey(t: Task, key: String): String = {
    "%d-%d.%s".format(t.context.partition.id, t.startNodeId, key);
  }

  def makekey(input: (Int, Int), key: String): String = {
    "%d-%d.%s".format(input._1, input._2, key);
  }
}