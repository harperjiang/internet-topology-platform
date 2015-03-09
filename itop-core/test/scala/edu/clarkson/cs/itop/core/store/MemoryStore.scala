package edu.clarkson.cs.itop.core.store

class MemoryStore extends KeyValueStore {

  var cache = scala.collection.mutable.Map[String, String]();

  var objCache = scala.collection.mutable.Map[String, AnyRef]();

  override def get(key: String): String = {
    cache.get(key).getOrElse(null)
  }

  override def set(key: String, value: String) = {
    cache.put(key, value);
  }

  override def getObject[T](key: String, clazz: Class[T]): T = {
    objCache.get(key).get.asInstanceOf[T];
  }

  override def setObject[T <: AnyRef](key: String, value: T): Unit = {
    objCache.put(key, value);
  }
}