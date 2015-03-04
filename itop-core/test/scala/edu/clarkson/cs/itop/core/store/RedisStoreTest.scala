package edu.clarkson.cs.itop.core.store

import java.util.UUID
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import javax.annotation.Resource
import redis.clients.jedis.Jedis
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

@RunWith(classOf[SpringJUnit4ClassRunner])
@ContextConfiguration(locations = Array("classpath:app-context-worker.xml"))
class RedisStoreTest {

  @Resource(name = "kvstore")
  var kvstore: RedisStore = null;

  @Test
  def testStoreAccess = {
    var value = UUID.randomUUID().toString();
    kvstore.set("TestKey", value);

    assertEquals(value, kvstore.get("TestKey"));
  }

}