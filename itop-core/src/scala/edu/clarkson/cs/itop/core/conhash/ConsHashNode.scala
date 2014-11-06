package edu.clarkson.cs.itop.core.conhash

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import edu.clarkson.cs.itop.core.conhash.message.CopyRequest
import edu.clarkson.cs.itop.core.conhash.message.QueryResponse
import edu.clarkson.cs.itop.core.conhash.message.Heartbeat
import edu.clarkson.cs.scala.common.message.Sender
import edu.clarkson.cs.itop.core.conhash.message.QueryRequest
import edu.clarkson.cs.itop.core.conhash.message.SetRequest
import org.springframework.beans.factory.InitializingBean
import scala.collection.mutable.ArrayBuffer
import edu.clarkson.cs.itop.core.conhash.message.CopyResponse
import java.util.HashSet
import edu.clarkson.cs.itop.core.conhash.message.StoreRemoveMessage
import edu.clarkson.cs.itop.core.conhash.message.StoreAddMessage

class ConsHashNode extends Sender with InitializingBean {

  var id = "";

  var dist: Distribution = null;

  var circle: HashCircle = null;

  var candidateCount = 3;

  var timeout = 1000;

  private var locks = new ConcurrentHashMap[String, Semaphore]();

  private var copyLocks = new ConcurrentHashMap[(String, BigDecimal), BigDecimal]();

  private var buffer = new ConcurrentHashMap[String, java.util.List[String]]();

  private var store: HashStore = null;

  def afterPropertiesSet(): Unit = {
    var locs = dist.idDist(id);
    store = new HashStore(locs);
  }

  /**
   * Interface for consistent hashing
   */
  def get(key: String): String = {
    var keyHash = dist.keyHash(key);
    var locations = circle.find(keyHash, candidateCount);
    var sessionKey = UUID.randomUUID().toString();
    var semaphore = new Semaphore(0);
    locks.put(sessionKey, semaphore);
    buffer.put(sessionKey, new ArrayBuffer[String]());
    locations.foreach { sendQuery(_, key, sessionKey) };
    semaphore.tryAcquire(candidateCount, timeout, TimeUnit.MILLISECONDS);
    return summarize(sessionKey);
  }

  private def summarize(sessionKey: String): String = {
    locks.remove(sessionKey);
    var results = buffer.remove(sessionKey);
    var counterMap = new java.util.HashMap[String, Int]();
    var candidate = "";
    var max = 0;
    if (results != null) {
      results.foreach(s => {
        var c = counterMap.getOrDefault(s, 1);
        c += 1;
        if (c > max) { max = c; candidate = s; }
        counterMap.put(s, c);
      });
    }
    // TODO When non-consistency is discovered, correct it
    return candidate;
  }

  def put(key: String, value: String): Unit = {
    var keyHash = dist.keyHash(key);
    var locations = circle.find(keyHash, candidateCount);
    locations.foreach { sendSet(_, key, value) };
  }

  /**
   * Message Communications
   */
  def sendHeartbeat = {
    var hb = new Heartbeat;
    send("ch.heartbeat", (hb, null));
  }

  def sendQuery(ref: (String, BigDecimal), key: String, sessionKey: String): Unit = {
    if (ref._1 == id) {
      // Local query
      onQueryResult(new QueryResponse(store.get(ref._2, key), sessionKey));
      return ;
    }

    var query = new QueryRequest(ref._1, ref._2, key, sessionKey);
    send("ch.query", (query, null));
  }

  def onQuery(request: QueryRequest) = {
    var result = store.get(request.location, request.key);
    var queryResp = new QueryResponse(result, request.sessionKey);
    send("ch.queryResp", (queryResp, null));
  }

  def onQueryResult(resp: QueryResponse) = {
    var results = buffer.get(resp.sessionKey);
    if (results != null) {
      results += resp.result;
    }
    var lock = locks.get(resp.sessionKey);
    if (lock != null)
      lock.release();
  }

  def sendSet(ref: (String, BigDecimal), key: String, value: String): Unit = {
    if (ref._1 == id) {
      // Local set
      store.put(ref._2, key, value);
      return ;
    }
    var request = new SetRequest(ref._1, ref._2, key, value);
    send("ch.set", (request, null));
  }

  def onSet(request: SetRequest): Unit = {
    if (request.nodeid != id)
      return ;
    store.put(request.location, request.key, request.value);
  }

  def onStoreAdded(message: StoreAddMessage) = {
    var locations = dist.idDist(message.storeId);
    circle.insert(locations, message.storeId);
  }

  def onStoreRemoved(message: StoreRemoveMessage) = {
    var locs = circle.remove(message.storeId);
    locs.foreach(f => {
      var loc = copyLocks.remove(f);
      if (loc != null) {
        // If there's ongoing copy command involved, restart it
        var newloc = circle.before(f._2);
        copy(newloc, loc);
      }
    })
  }

  def copy(from: (String, BigDecimal), to: BigDecimal) = {
    copyLocks.put(from, to);
    var copyRequest = new CopyRequest();
    copyRequest.fromLocation = from._2;
    copyRequest.toNode = id;
    copyRequest.toLocation = to;

    send("ch.copy", (copyRequest, (m) => { m.setStringProperty("node_id", from._1) }));
  }

  def onCopyRequest(request: CopyRequest) = {
    var copyResponse = new CopyResponse();
    copyResponse.toLocation = request.toLocation;
    copyResponse.fromLocation = request.fromLocation;
    copyResponse.fromNode = id;
    copyResponse.content = store.getAll(request.fromLocation);
    send("ch.copyResp", (copyResponse, (m) => { m.setStringProperty("node_id", request.toNode) }))
  }

  def onCopyResponse(resp: CopyResponse) = {
    store.getAll(resp.toLocation).putAll(resp.content);
    copyLocks.remove((resp.fromNode, resp.fromLocation));
  }
}