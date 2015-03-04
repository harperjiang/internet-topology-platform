package edu.clarkson.cs.itop.core.store

import org.springframework.data.redis.core.RedisTemplate

class RedisStore extends KeyValueStore {

  var template: RedisTemplate[String, String] = null;

  def get(key: String): String = {
    template.opsForValue().get(key)
  }

  def set(key: String, value: String): Unit = {
    template.opsForValue().set(key, value);
  }
}