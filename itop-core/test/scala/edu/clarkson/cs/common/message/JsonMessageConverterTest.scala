package edu.clarkson.cs.common.message

import org.apache.activemq.command.ActiveMQTextMessage
import org.junit.Test
import org.junit.Assert._
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import com.google.gson.Gson
import javax.annotation.Resource
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import edu.clarkson.cs.scala.common.message.JsonMessageConverter;
import edu.clarkson.cs.itop.core.dist.message.Heartbeat

@RunWith(classOf[SpringJUnit4ClassRunner])
@ContextConfiguration(locations = Array("classpath:jms.xml"))
class JsonMessageConverterTest {

  @Resource(name = "gsonTranslator")
  var gson: Gson = null;

  @Test
  def testFromMessage() = {
    var mc = new JsonMessageConverter();
    mc.translator = gson;
    
    var hb = new Heartbeat(3, 5);

    var json = gson.toJsonTree(hb);
    json.getAsJsonObject.addProperty("class", classOf[Heartbeat].getName);
    var message = new ActiveMQTextMessage();
    message.setText(json.toString());

    var result = mc.fromMessage(message);

    assertEquals(classOf[Heartbeat], result.getClass);
  }
}