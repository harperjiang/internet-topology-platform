package edu.clarkson.cs.itop.core.conhash

import edu.clarkson.cs.itop.core.conhash.message.Heartbeat
import edu.clarkson.cs.scala.common.message.Sender
import org.springframework.beans.factory.InitializingBean
import edu.clarkson.cs.itop.core.conhash.message.StoreRemoveMessage
import edu.clarkson.cs.itop.core.conhash.message.StoreAddMessage
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.ReentrantLock

class ConsHashMaster extends Sender with InitializingBean {

  var interval = 5000l;

  var monitorInterval = 1000l;

  private val stores = new ConcurrentHashMap[String, Long];

  private val logger = LoggerFactory.getLogger(getClass());

  private val lock = new ReentrantLock();

  def afterPropertiesSet(): Unit = {
    new MonitorThread().start;
  }

  def checkDeath() = {
    var now = System.currentTimeMillis();
    var deathCandidates = new ArrayBuffer[String];
    stores.foreach(entry => {
      if (entry._2 + interval < now) {
        // Death candidates found
        deathCandidates += entry._1;
      }
    });
    if (!deathCandidates.isEmpty) {
      lock.lock();
      deathCandidates.foreach(key => {
        if (stores.get(key) + interval < now) {
          stores.remove(key);
          sendStoreRemove(key);
        }
      });
      lock.unlock();
    }
  }

  def onHeartbeat(hb: Heartbeat) = {
    if (!stores.containsKey(hb.storeId)) {
      // Neophyte found
      lock.lock();
      if (!stores.containsKey(hb.storeId)) {
        stores.put(hb.storeId, System.currentTimeMillis());
        sendStoreAdd(hb.storeId);
      }
      lock.unlock();
    } else {
      stores.put(hb.storeId, System.currentTimeMillis());
    }
  }

  def sendStoreRemove(storeId: String) = {
    send("ch.store", (new StoreRemoveMessage(storeId), null))
  }

  def sendStoreAdd(storeId: String) = {
    send("ch.store", (new StoreAddMessage(storeId), null))
  }

  class MonitorThread extends Thread {
    {
      setName("ConsHashMaster-Monitor");
      setDaemon(true);
    }

    override def run(): Unit = {
      while (true) {
        try {
          checkDeath();
          Thread.sleep(monitorInterval);
        } catch {
          case e: Exception => {
            logger.error("Exception in monitorThread", e);
          }
        }
      }
    }
  }
}