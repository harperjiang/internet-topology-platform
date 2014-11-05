package edu.clarkson.cs.itop.core.dist

import org.junit.Test
import org.junit.Assert._
import org.junit.Before
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

import javax.annotation.Resource

import edu.clarkson.cs.itop.core.dist.message.Heartbeat

@RunWith(classOf[SpringJUnit4ClassRunner])
@ContextConfiguration(locations = Array("classpath:app-context-master.xml"))
class MasterNodeTest {

  @Resource(name = "masterNode")
  var masterNode: MasterNode = null;

  @Test
  def testInvalidRange = {
    try {
      masterNode.onHeartbeat(new Heartbeat(0, -1));
      fail("");
    } catch {
      case ex: Exception => {
      }
    }
    try {
      masterNode.onHeartbeat(new Heartbeat(0, 11));
      fail("");
    } catch {
      case ex: Exception => {
      }
    }
    masterNode.onHeartbeat(new Heartbeat(0, 10));
  }

  @Test
  def testStateChange = {
    assertEquals(MachineStatus.UNKNOWN, masterNode.status(1));
    masterNode.onHeartbeat(new Heartbeat(0, 1))
    assertEquals(MachineStatus.UNSTABLE, masterNode.status(1));
    for (i <- 1 to 59) {
      masterNode.onHeartbeat(new Heartbeat(0, 1))
      assertEquals(MachineStatus.UNSTABLE, masterNode.status(1));
    }
    masterNode.onHeartbeat(new Heartbeat(0, 1))
    assertEquals(MachineStatus.NORMAL, masterNode.status(1));

    Thread.sleep(3000);
    masterNode.onHeartbeat(new Heartbeat(0, 1))
    assertEquals(MachineStatus.UNSTABLE, masterNode.status(1));

    Thread.sleep(6000);
    for (i <- 0 to 10)
      assertEquals(MachineStatus.DOWN, masterNode.status(9));

    masterNode.onHeartbeat(new Heartbeat(0, 1))
    assertEquals(MachineStatus.UNSTABLE, masterNode.status(1));
  }

}