package edu.clarkson.cs.itop.model.routing

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test


class DefaultRoutingTest {

  @Test
  def testLoadRouting: Unit = {
    var routing = new DefaultRouting;
    routing.routingSize = 10;
    routing.routingFile = "testdata/routing";

    routing.afterPropertiesSet;

    assertTrue(routing.route(5).isEmpty)
    assertEquals(1, routing.route(7).toList(0));
    assertEquals(2, routing.route(7).toList(1));
    assertTrue(routing.route(1).isEmpty)
    assertEquals(1, routing.route(8).toList(0));
    assertEquals(2, routing.route(8).toList(1));
    
  }
}