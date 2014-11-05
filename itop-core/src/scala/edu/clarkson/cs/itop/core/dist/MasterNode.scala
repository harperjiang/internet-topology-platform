package edu.clarkson.cs.itop.core.dist

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions.collectionAsScalaIterable
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.InitializingBean
import edu.clarkson.cs.itop.core.dist.message.Heartbeat


/**
 * <code>MasterNode</code> is the control plane of the system. It can report status of each machine.
 */
class MasterNode extends InitializingBean {

  private val machineStatus = new ConcurrentHashMap[Int, MachineState]();
  private val logger = LoggerFactory.getLogger(getClass());

  var maxMachineId = 0;
  var abnormalInterval = 0L;
  var downInterval = 0L;
  var stableCount = 0;

  def afterPropertiesSet() = {
    for (i <- 1 to maxMachineId)
      machineStatus.put(i, new MachineState(MachineStatus.UNKNOWN, -1, 0));

    // Start a monitor thread
    new DaemonThread().start();
  }

  def status(machineId: Int): MachineStatus.T = {
    if (machineId > maxMachineId || machineId < 0)
      throw new IllegalArgumentException("Out of range");
    return machineStatus.get(machineId).status;
  }

  def onHeartbeat(hb: Heartbeat): Unit = {
    if (hb.machineId > maxMachineId || hb.machineId < 0) {
      var msg = "Invalid machine id received:%d".format(hb.machineId)
      logger.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    val record = machineStatus.get(hb.machineId);

    record.synchronized({
      update(record);
    })
  }

  private def update(record: MachineState): Unit = {
    val current = System.currentTimeMillis();
    val interval = current - record.lastUpdate;
    record.status match {
      case MachineStatus.UNKNOWN => { record.update(MachineStatus.UNSTABLE, current, 1) }
      case MachineStatus.NORMAL => {
        if (interval < abnormalInterval)
          record.update(MachineStatus.NORMAL, current, record.counter + 1)
        else record.update(MachineStatus.UNSTABLE, current, 1)
      }
      case MachineStatus.DOWN => { record.update(MachineStatus.UNSTABLE, current, 1) }
      case MachineStatus.UNSTABLE => {
        if (interval < abnormalInterval) {
          if (record.counter >= stableCount) record.update(MachineStatus.NORMAL, current, 1)
          else record.update(MachineStatus.UNSTABLE, current, record.counter + 1)
        } else record.update(MachineStatus.UNSTABLE, current, 1);
      }
    }
  }

  private def detectdown = {
    val current = System.currentTimeMillis();
    for (record <- machineStatus.values()) {
      record.synchronized({
        if (current - record.lastUpdate > downInterval) {
          record.update(MachineStatus.DOWN, current, 1)
        }
      })
    }
  }

  class DaemonThread extends Thread {

    private val interval = 1000;
    setDaemon(true);
    setName("MasterNode-DaemonThread");

    override def run = {
      while (true) {
        try {
          Thread.sleep(interval);
          detectdown;
        } catch {
          case e: Exception => {
            logger.error("Exception in MasterNode-Daemon Thread", e);
          }
        }
      }
    }
  }
}

class MachineState(mt: MachineStatus.T, lu: Long, c: Int) {
  var status: MachineStatus.T = mt
  var lastUpdate = lu
  var counter = c

  def update(mt: MachineStatus.T, lu: Long, c: Int) = {
    status = mt;
    lastUpdate = lu;
    counter = c;
  }
}

object MachineStatus extends Enumeration {
  type T = Value
  val NORMAL, DOWN, UNSTABLE, UNKNOWN = Value
};