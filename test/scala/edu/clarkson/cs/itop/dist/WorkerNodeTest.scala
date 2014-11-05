package edu.clarkson.cs.itop.dist

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import javax.annotation.Resource
import javax.jms.TextMessage
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import edu.clarkson.cs.common.test.message.DummyJmsTemplate

@RunWith(classOf[SpringJUnit4ClassRunner])
@ContextConfiguration(locations = Array("classpath:app-context-worker.xml"))
class WorkerNodeTest {

  @Resource(name = "workerNode")
  var workerNode: WorkerNode = null;

  @Resource(name = "jmsTemplate")
  var djt: DummyJmsTemplate = null;

  @Test
  def testHeartbeat = {
    djt.getStorage().get("heartbeatDest").clear();
    Thread.sleep(5000);
    assertEquals(3, djt.getStorage().get("heartbeatDest").size());

    var tm = djt.getStorage().get("heartbeatDest").poll().asInstanceOf[TextMessage];

    assertEquals("{\"machine_id\":1,\"group_id\":1,\"class\":\"edu.clarkson.cs.itop.dist.message.Heartbeat\"}", tm.getText());
  }

}