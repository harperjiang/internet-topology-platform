package edu.clarkson.cs.itop.core.conhash

import edu.clarkson.cs.scala.common.message.Sender
import edu.clarkson.cs.itop.core.conhash.message.Heartbeat
import java.util.concurrent.ConcurrentHashMap

class ConsHashNode extends Sender {

  var id = 0;

  var localStorage = new ConcurrentHashMap[String, String]();

  /**
   * Communication messages
   */
  def sendHeartbeat = {
    var hb = new Heartbeat;
    send("ch.heartbeat", (hb, null));
  }
}
