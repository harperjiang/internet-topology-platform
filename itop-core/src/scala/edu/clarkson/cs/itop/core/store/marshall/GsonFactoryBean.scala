package edu.clarkson.cs.itop.core.store.marshall

import org.springframework.beans.factory.FactoryBean

import com.google.gson.Gson
import com.google.gson.GsonBuilder

class GsonFactoryBean extends FactoryBean[Gson] {

  var parts: Array[SDPart] = null;

  def getObject(): Gson = {
    var builder = new GsonBuilder();

    for (p <- parts) {
      builder.registerTypeAdapter(p.clazz, p.part)
    }

    return builder.create();
  }

  def getObjectType = classOf[Gson]

  def isSingleton() = false
}

class SDPart {
  var clazz: Class[AnyRef] = null;
  var part: AnyRef = null;
}