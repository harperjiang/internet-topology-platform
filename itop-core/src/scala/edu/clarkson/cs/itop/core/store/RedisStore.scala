package edu.clarkson.cs.itop.core.store

import org.springframework.data.redis.core.RedisTemplate
import edu.clarkson.cs.itop.core.store.marshall.Marshaller

class RedisStore extends KeyValueStore {

  var template: RedisTemplate[String, String] = null;

  var marshaller: Marshaller = null;

  override def get(key: String): String = {
    template.opsForValue().get(key)
  }

  override def set(key: String, value: String): Unit = {
    template.opsForValue().set(key, value);
  }

  override def getObject[T](key: String, clazz: Class[T]): T = {
    if (marshaller != null)
      return marshaller.unmarshall(get(key), clazz)
    throw new UnsupportedOperationException("Missing marshaller");
  }

  override def setObject[T](key: String, value: T): Unit = {
    if (marshaller != null)
      set(key, marshaller.marshall(value));
    else
      throw new UnsupportedOperationException("Missing marshaller");
  }
}