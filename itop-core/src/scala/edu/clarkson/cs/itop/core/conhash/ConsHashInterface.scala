package edu.clarkson.cs.itop.core.conhash

import edu.clarkson.cs.itop.core.conhash.message.QueryResponse
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore

class ConsHashInterface {

  var dist: Distribution = null;

  var circle: HashCircle = null;

  var candidateCount = 3;

  var locks = new ConcurrentHashMap[String, Semaphore]();

  var buffer = new ConcurrentHashMap[String, java.util.List[String]]();
  /**
   * Interface for consistent hashing
   */
  def get(key: String): String = {
    var keyHash = dist.keyHash(key);
    var refs = circle.find(keyHash, candidateCount);
    var sessionKey = UUID.randomUUID().toString();
    var semaphore = new Semaphore(0);
    locks.put(sessionKey, semaphore);
    refs.foreach { ref =>
      {
        sendQuery(ref, key, sessionKey);
      }
    };
    semaphore.acquire(candidateCount);
    return summarize(sessionKey);
  }

  private def summarize(sessionKey: String): String = {
    throw new RuntimeException("Not implemented");
  }

  def put(key: String, value: String): Unit = {
    var keyHash = dist.keyHash(key);
    var refs = circle.find(keyHash, candidateCount); refs.foreach { ref =>
      {
        sendSet(ref, key, value);
      }
    };
  }

  /**
   * Message Communications
   */
  def sendQuery(ref: Int, key: String, sessionKey: String) = {

  }

  def onQueryResult(result: QueryResponse) = {

  }

  def sendSet(ref: Int, key: String, value: String) = {

  }
}