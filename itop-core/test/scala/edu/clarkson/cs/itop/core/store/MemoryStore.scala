package edu.clarkson.cs.itop.core.store

class MemoryStore extends KeyValueStore {

  var cache = scala.collection.mutable.Map[String, String]();

  override def get(key: String): String = {
    cache.get(key).getOrElse(null)
  }
  override def set(key: String, value: String) = {
    cache.put(key, value);
  }
}