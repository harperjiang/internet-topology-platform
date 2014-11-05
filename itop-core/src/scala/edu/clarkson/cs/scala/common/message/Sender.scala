package edu.clarkson.cs.scala.common.message

import org.springframework.jms.core.JmsTemplate
import org.springframework.jms.core.MessageCreator
import javax.jms.Session
import javax.jms.Message
import com.google.gson.Gson

trait Sender {

  var jmsTemplate: JmsTemplate = null;

  def send(dest: String, message: (Object, (Message => Unit))) = {
    jmsTemplate.convertAndSend(dest, message)
  }
}
