package edu.clarkson.cs.itop.core.conhash

import edu.clarkson.cs.scala.common.message.Sender
import edu.clarkson.cs.itop.core.conhash.message.Heartbeat
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._

class HashStore {

  private val store = new ConcurrentHashMap[BigDecimal, java.util.Map[String, String]];

  def this(locations: Iterable[BigDecimal]) = {
    this();
    locations.foreach(store.put(_, new ConcurrentHashMap[String, String]()))
  }

  def get(location: BigDecimal, key: String): String = {
    return store.get(location).get(key);
  }

  def getAll(location: BigDecimal): java.util.Map[String, String] = {
    return store.get(location);
  }

  def put(location: BigDecimal, key: String, value: String) = {
    store.get(location).put(key, value);
  }
}
