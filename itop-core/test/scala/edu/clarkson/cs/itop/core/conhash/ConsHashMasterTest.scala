package edu.clarkson.cs.itop.core.conhash

import org.junit.Before
import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.common.test.message.DummyJmsTemplate
import edu.clarkson.cs.itop.core.conhash.message.Heartbeat
import edu.clarkson.cs.scala.common.message.JsonMessageConverter
import edu.clarkson.cs.itop.core.dist.message.GsonFactoryBean
import com.google.gson.Gson
import javax.jms.TextMessage

class ConsHashMasterTest {

  var master: ConsHashMaster = null;

  var jmsTemplate: DummyJmsTemplate = null;

  @Before
  def prepare = {
    master = new ConsHashMaster
    master.interval
    jmsTemplate = new DummyJmsTemplate
    var messageConverter = new JsonMessageConverter;
    var gson = new GsonFactoryBean().getObject.asInstanceOf[Gson];
    messageConverter.translator = gson;
    jmsTemplate.setMessageConverter(messageConverter);
    master.jmsTemplate = jmsTemplate
    master.afterPropertiesSet
  }

  @Test
  def testHeartbeat = {
    // Should have one addstore for this
    master.onHeartbeat(new Heartbeat("node1", System.currentTimeMillis()));
    // Should have no addstore message for this
    master.onHeartbeat(new Heartbeat("node1", System.currentTimeMillis()));
    // Should have another addstore message for this
    master.onHeartbeat(new Heartbeat("node2", System.currentTimeMillis()));

    Thread.sleep(7000l);
    // Should have two removestore message now

    var addQueue = jmsTemplate.getStorage().get("ch.store");

    assertEquals(4, addQueue.size());
    assertEquals("{\"storeId\":\"node1\",\"class\":\"edu.clarkson.cs.itop.core.conhash.message.StoreAddMessage\"}", addQueue.poll.asInstanceOf[TextMessage].getText());
    assertEquals("{\"storeId\":\"node2\",\"class\":\"edu.clarkson.cs.itop.core.conhash.message.StoreAddMessage\"}", addQueue.poll.asInstanceOf[TextMessage].getText());
    assertEquals("{\"storeId\":\"node2\",\"class\":\"edu.clarkson.cs.itop.core.conhash.message.StoreRemoveMessage\"}", addQueue.poll.asInstanceOf[TextMessage].getText());
    assertEquals("{\"storeId\":\"node1\",\"class\":\"edu.clarkson.cs.itop.core.conhash.message.StoreRemoveMessage\"}", addQueue.poll.asInstanceOf[TextMessage].getText());
  }
}