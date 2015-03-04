package edu.clarkson.cs.itop.core.task

import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.core.store.MemoryStore

class TaskContextTest {

  @Test
  def testContextGetSet = {
    var kvstore = new MemoryStore
    var taskContext = new TaskContext((1, "SID"), null, null, kvstore);

    taskContext.set("key", "value");

    assertFalse(kvstore.cache.contains("key"));
    assertTrue(kvstore.cache.contains("1-SID-key"))
    assertEquals("value", kvstore.cache.get("1-SID-key").get)
    assertEquals("value", taskContext.get("key"))
  }
}