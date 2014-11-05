package edu.clarkson.cs.scala.common.message

import java.lang.reflect.Type

import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.mapAsScalaMap

import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer

class KVStore {
  var map: java.util.Map[String, Object] = new java.util.HashMap[String, Object]();
}

class KVStoreSD extends JsonSerializer[KVStore] with JsonDeserializer[KVStore] {
  def serialize(src: KVStore, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    return context.serialize(src.map);
  }

  def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): KVStore = {
    if (!json.isJsonObject())
      throw new IllegalArgumentException("Not a json object");
    var store = new KVStore;
    store.map = context.deserialize(json, classOf[java.util.Map[String, Object]]);
    return store;
  }
}
