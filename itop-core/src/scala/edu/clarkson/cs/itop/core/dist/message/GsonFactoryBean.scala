package edu.clarkson.cs.itop.core.dist.message

import org.springframework.beans.factory.FactoryBean
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import edu.clarkson.cs.httpjson.json.BeanDeserializer
import edu.clarkson.cs.httpjson.json.BeanSerializer
import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.scala.common.message.KVStoreSD
import edu.clarkson.cs.httpjson.json.BeanDeserializer
import edu.clarkson.cs.scala.common.message.KVStoreSD
import edu.clarkson.cs.scala.common.message.KVStore
import edu.clarkson.cs.httpjson.json.BeanSerializer

class GsonFactoryBean extends FactoryBean[Gson] {

  def getObject(): Gson = {
    var builder = new GsonBuilder();

    builder.registerTypeAdapter(classOf[Heartbeat], new BeanSerializer[Heartbeat]);
    builder.registerTypeAdapter(classOf[Heartbeat], new BeanDeserializer[Heartbeat]);
    builder.registerTypeAdapter(classOf[SubtaskExecute], new BeanSerializer[SubtaskExecute]);
    builder.registerTypeAdapter(classOf[SubtaskExecute], new BeanDeserializer[SubtaskExecute]);
    builder.registerTypeAdapter(classOf[SubtaskResult], new BeanSerializer[SubtaskResult]);
    builder.registerTypeAdapter(classOf[SubtaskResult], new BeanDeserializer[SubtaskResult]);

    builder.registerTypeAdapter(classOf[KVStore], new KVStoreSD);
    
    return builder.create();
  }

  def getObjectType() = classOf[Gson];

  def isSingleton() = false;
}