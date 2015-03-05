package edu.clarkson.cs.itop.core.store.marshall

import org.springframework.beans.factory.FactoryBean
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import scala.collection.JavaConversions._

class GsonFactoryBean extends FactoryBean[Gson] {

  def getObject(): Gson = {
    var builder = new GsonBuilder();
    return builder.create();
  }

  def getObjectType = classOf[Gson]

  def isSingleton() = false
}