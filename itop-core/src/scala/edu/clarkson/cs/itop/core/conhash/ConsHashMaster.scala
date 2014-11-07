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

class ConsHashMaster extends Sender with InitializingBean {

  var interval = 10000l;

  var monitorInterval = 1000l;

  var stores = new ConcurrentHashMap[String, Long];

  private val logger = LoggerFactory.getLogger(getClass());

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
    deathCandidates.foreach(key => {
      stores.remove(key);
      sendStoreRemove(key);
    })
  }

  def onHeartbeat(hb: Heartbeat) = {
    if (!stores.contains(hb.storeId)) {
      // Neophyte found
      sendStoreAdd(hb.storeId);
      // We don't put any lock here cause it doesn't matter 
      // even if the store add message is sent multiple times.
    }
    stores.put(hb.storeId, System.currentTimeMillis());
  }

  def sendStoreRemove(storeId: String) = {
    send("ch.storeRemove", (new StoreRemoveMessage(storeId), null))
  }

  def sendStoreAdd(storeId: String) = {
    send("ch.storeAdd", (new StoreAddMessage(storeId), null))
  }

  class MonitorThread extends Thread {
    {
      setName("ConsHashMaster-Monitor");
      setDaemon(true);
    }

    override def run(): Unit = {
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