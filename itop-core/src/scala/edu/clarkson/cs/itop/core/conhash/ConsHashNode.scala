package edu.clarkson.cs.itop.core.conhash

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.InitializingBean
import edu.clarkson.cs.itop.core.conhash.message.CopyRequest
import edu.clarkson.cs.itop.core.conhash.message.CopyResponse
import edu.clarkson.cs.itop.core.conhash.message.Heartbeat
import edu.clarkson.cs.itop.core.conhash.message.QueryRequest
import edu.clarkson.cs.itop.core.conhash.message.QueryResponse
import edu.clarkson.cs.itop.core.conhash.message.SetRequest
import edu.clarkson.cs.itop.core.conhash.message.StoreAddMessage
import edu.clarkson.cs.itop.core.conhash.message.StoreMessage
import edu.clarkson.cs.itop.core.conhash.message.StoreRemoveMessage
import edu.clarkson.cs.itop.core.conhash.message.SyncCircleRequest
import edu.clarkson.cs.itop.core.conhash.message.SyncCircleResponse
import edu.clarkson.cs.scala.common.message.Sender
import scala.io.Source

class ConsHashNode extends Sender with InitializingBean {

  var id = "";

  var function: HashFunction = null;

  var circle: HashCircle = null;

  var candidateCount = 3;

  var heartbeatInterval = 1000l;

  var copyTimeout = 10000l;

  var timeout = 1000l;

  var dataFile = "";

  private val logger = LoggerFactory.getLogger(getClass());

  private var locks = new ConcurrentHashMap[String, Semaphore]();

  private var copyLocks = new ConcurrentHashMap[(String, BigDecimal), BigDecimal]();

  private var buffer = new ConcurrentHashMap[String, java.util.List[(String, String, BigDecimal)]]();

  private var store: HashStore = null;

  def afterPropertiesSet(): Unit = {
    var locs = function.idDist(id);
    store = new HashStore(locs);
    if (!dataFile.isEmpty())
      load;
  }

  /**
   * Interface for consistent hashing
   */
  def get(key: String): String = {
    var keyHash = function.keyHash(key);
    var locations = circle.find(keyHash, candidateCount);
    var sessionKey = UUID.randomUUID().toString();
    var semaphore = new Semaphore(0);
    locks.put(sessionKey, semaphore);
    buffer.put(sessionKey, new ArrayBuffer[(String, String, BigDecimal)]());
    locations.foreach { sendQuery(_, key, sessionKey) };
    semaphore.tryAcquire(candidateCount, timeout, TimeUnit.MILLISECONDS);
    return summarize(key, sessionKey);
  }

  private def summarize(key: String, sessionKey: String): String = {
    locks.remove(sessionKey);
    var results = buffer.remove(sessionKey);
    var counterMap = scala.collection.mutable.Map[String, Int]();
    if (results == null) {
      return null;
    }
    // Null result will be ignored
    var lookformax = new Function0[String] {
      def apply(): String = {
        var candidate: String = null;
        var max = 0;
        results.foreach(s => {
          var c = counterMap.getOrElse(s._1, 0);
          c += 1;
          if (c > max) { max = c; candidate = s._1; }
          counterMap.put(s._1, c);
          if (c > results.size() / 2)
            return s._1;
        });
        return candidate;
      };
    }
    var result = lookformax();

    // When non-consistency is discovered, correct it
    results.foreach(s => {
      if (s._1 != result) {
        sendSet((s._2, s._3), key, result);
      }
    });
    return result;
  }

  def put(key: String, value: String): Unit = {
    var keyHash = function.keyHash(key);
    var locations = circle.find(keyHash, candidateCount);
    locations.foreach { sendSet(_, key, value) };
  }

  /**
   * Message Communications
   */
  private def sendHeartbeat = {
    var hb = new Heartbeat(id, System.currentTimeMillis());
    send("ch.heartbeat", (hb, null));
  }

  private def sendQuery(ref: (String, BigDecimal), key: String, sessionKey: String): Unit = {
    if (ref._1 == id) {
      // Local query, fake a result
      onQueryResult(new QueryResponse(id, ref._2, store.get(ref._2, key), sessionKey));
      return ;
    }

    var query = new QueryRequest(ref._1, ref._2, key, sessionKey);
    send("ch.query", (query, null));
  }

  def onQuery(request: QueryRequest) = {
    var result = store.get(request.location, request.key);
    var queryResp = new QueryResponse(id, request.location, result, request.sessionKey);
    send("ch.queryResp", (queryResp, null));
  }

  def onQueryResult(resp: QueryResponse) = {
    var results = buffer.get(resp.sessionKey);
    if (results != null) {
      results += ((resp.result, resp.nodeid, resp.fromloc));
    }
    var lock = locks.get(resp.sessionKey);
    if (lock != null)
      lock.release();
  }

  private def sendSet(ref: (String, BigDecimal), key: String, value: String): Unit = {
    if (ref._1 == id) {
      // Local set
      store.put(ref._2, key, value);
      return ;
    }
    var request = new SetRequest(ref._1, ref._2, key, value);
    send("ch.set", (request, m => { m.setStringProperty("node_id", ref._1) }));
  }

  def onSet(request: SetRequest): Unit = {
    if (request.nodeid != id)
      throw new IllegalArgumentException("Not for this node");
    store.put(request.location, request.key, request.value);
  }

  def onStoreChanged(message: StoreMessage) = {
    message match {
      case e: StoreAddMessage => {
        var locations = function.idDist(e.storeId);
        circle.insert(locations, e.storeId);
      }
      case e: StoreRemoveMessage => {
        var locs = circle.remove(e.storeId);
        locs.foreach(f => {
          var loc = copyLocks.remove(f);
          if (loc != null) {
            // If there's ongoing copy command involved, restart it
            var newloc = circle.before(f._2);
            newloc match {
              case Some(a) => sendCopy(a, loc);
              case _ =>
            }
          }
        })
      }
    }
  }

  private val copyCounter = new Semaphore(0);

  private def makeCopy: Int = {
    var locs = function.idDist(id);
    var counter = 0;
    locs.foreach(loc => {
      var copyfrom = circle.before(loc)
        .getOrElse(throw new IllegalArgumentException("Empty circle"));
      for (i <- 1 to candidateCount - 1) {
        if (copyfrom._1 != id) {
          // Not local
          counter += 1;
          sendCopy(copyfrom, loc);
        }
        copyfrom = circle.before(copyfrom._2)
          .getOrElse(throw new IllegalArgumentException("Empty circle"));
      }
    });
    return counter;
  }

  private def sendCopy(from: (String, BigDecimal), to: BigDecimal) = {
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
    copyCounter.release();
  }

  private var syncLock: Semaphore = new Semaphore(0);

  private var syncDoneLock: Semaphore = new Semaphore(0);

  private def requestCircleSync() = {
    syncLock.drainPermits();
    syncLock.release();
    send("ch.sync", (new SyncCircleRequest(id), null));
  }

  def onSyncCircleRequest(req: SyncCircleRequest) = {
    var resp = new SyncCircleResponse();
    resp.circle = circle.content;
    send("ch.syncResp", (resp, null));
  }

  def onSyncCircleResponse(resp: SyncCircleResponse) = {
    if (syncLock.tryAcquire()) {
      resp.circle.foreach(id => {
        circle.insert(function.idDist(id), id);
      });
      syncDoneLock.release();
    }
  }

  /**
   * Newly started node.
   */
  def start = {
    new HeartbeatThread().start();
  }

  /**
   * Every newly joined node should invoke this
   */
  def join = {
    requestCircleSync();
    // When sync is done, copy data from other nodes
    if (!syncDoneLock.tryAcquire(timeout, TimeUnit.MILLISECONDS))
      throw new RuntimeException("Failed to sync circle");
    // For each id, check and copy from locations before it
    var size = makeCopy;
    if (!copyCounter.tryAcquire(size, copyTimeout, TimeUnit.MILLISECONDS)) {
      throw new RuntimeException("Failed to copy data, timeout");
    }
    // Add self id locally, remote will be handled when heartbeat sent
    circle.insert(function.idDist(id), id);
    new HeartbeatThread().start();
  }

  /**
   * If data file is ready, load that into datastore.
   * The file should be in the following format and we trust the data file provider.
   * <location> <key> <value>
   */
  def load() {
    Source.fromFile(dataFile).getLines().filter(!_.startsWith("#")).foreach { line =>
      {
        try {
          var parts = line.split("\\s");
          store.put(BigDecimal(parts(0).toInt), parts(1), parts(2));
        } catch {
          case e: NumberFormatException => {
            logger.error("Number format error in data file", line);
          }
        }
      }
    };
  }

  class HeartbeatThread extends Thread {

    setName("ConsHashNode-Heartbeat");
    setDaemon(true);

    override def run = {
      while (true) {
        try {
          sendHeartbeat;
          Thread.sleep(heartbeatInterval);
        } catch {
          case e: Exception => {
            logger.error("Exception in heartbeat thread", e);
          }
        }
      }
    }
  }
}