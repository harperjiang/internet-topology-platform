package edu.clarkson.cs.itop.core.store.marshall

import com.google.gson.Gson

class JsonMarshaller extends Marshaller {

  var gson: Gson = null;

  override def marshall[T](obj: T): String = {
    gson.toJson(obj).toString
  }

  override def unmarshall[T](data: String, clazz: Class[T]): T = {
    gson.fromJson(data, clazz);
  }
}