package edu.clarkson.cs.scala.common.message

import javax.jms.Session
import org.springframework.jms.support.converter.MessageConverter
import javax.jms.Message
import com.google.gson.Gson
import scala.Tuple2
import javax.jms.TextMessage
import com.google.gson.JsonParser
import com.google.gson.JsonObject

class JsonMessageConverter extends MessageConverter {

  var translator: Gson = null;

  private val parser = new JsonParser();

  def fromMessage(message: Message): Object = {
    if (message.isInstanceOf[TextMessage]) {
      var tm = message.asInstanceOf[TextMessage];
      var jsonElem = parser.parse(tm.getText());
      if (jsonElem.isInstanceOf[JsonObject]) {
        var jsonobj = jsonElem.getAsJsonObject();
        var classStr = jsonobj.get("class").getAsString();
        jsonobj.remove("class");
        return translator.fromJson(jsonElem, Class.forName(classStr));
      } else {
        return jsonElem.getAsString();
      }
    }
    return null;
  }

  def toMessage(obj: Object, session: Session): Message = {
    var tuple = obj.asInstanceOf[Tuple2[Object, Message => Unit]];
    var json = translator.toJsonTree(tuple._1);
    if (json.isInstanceOf[JsonObject]) {
      json.asInstanceOf[JsonObject]
        .addProperty("class", tuple._1.getClass().getName());
    }
    var msg = session.createTextMessage(json.toString());
    if (tuple._2 != null)
      tuple._2(msg);
    return msg;
  }
}