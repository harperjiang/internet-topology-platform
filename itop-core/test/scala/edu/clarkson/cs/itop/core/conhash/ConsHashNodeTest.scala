package edu.clarkson.cs.itop.core.conhash

import org.junit.Before
import org.junit.Test

import com.google.gson.Gson

import edu.clarkson.cs.common.test.message.DummyJmsTemplate
import edu.clarkson.cs.itop.core.dist.message.GsonFactoryBean
import edu.clarkson.cs.scala.common.message.JsonMessageConverter

class ConsHashNodeTest {

  var node: ConsHashNode = null;
  var jmsTemplate: DummyJmsTemplate = null;

  @Before
  def prepare = {
    node = new ConsHashNode
    node.function = new DefaultHashFunction;
    node.circle = new TreeMapHashCircle;
    jmsTemplate = new DummyJmsTemplate
    var messageConverter = new JsonMessageConverter;
    var gson = new GsonFactoryBean().getObject.asInstanceOf[Gson];
    messageConverter.translator = gson;
    jmsTemplate.setMessageConverter(messageConverter);
    node.jmsTemplate = jmsTemplate;
    node.afterPropertiesSet
  }

  @Test
  def testGet = {

  }

  @Test
  def testSet = {

  }
}