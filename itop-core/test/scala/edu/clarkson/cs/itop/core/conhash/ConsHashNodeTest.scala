package edu.clarkson.cs.itop.core.conhash

import scala.BigDecimal
import scala.collection.JavaConversions.setAsJavaSet

import org.junit.Assert._
import org.junit.Before
import org.junit.Test

import com.google.gson.Gson

import edu.clarkson.cs.common.test.message.DummyJmsTemplate
import edu.clarkson.cs.itop.core.conhash.message.CopyResponse
import edu.clarkson.cs.itop.core.conhash.message.Heartbeat
import edu.clarkson.cs.itop.core.conhash.message.QueryRequest
import edu.clarkson.cs.itop.core.conhash.message.QueryResponse
import edu.clarkson.cs.itop.core.conhash.message.SetRequest
import edu.clarkson.cs.itop.core.conhash.message.StoreAddMessage
import edu.clarkson.cs.itop.core.conhash.message.StoreRemoveMessage
import edu.clarkson.cs.itop.core.conhash.message.SyncCircleRequest
import edu.clarkson.cs.itop.core.conhash.message.SyncCircleResponse
import edu.clarkson.cs.itop.core.dist.message.GsonMessageFactoryBean
import edu.clarkson.cs.scala.common.message.JsonMessageConverter

class ConsHashNodeTest {

  var node: ConsHashNode = null;
  var jmsTemplate: DummyJmsTemplate = null;
  var messageConverter: JsonMessageConverter = null;
  var circle: DummyCircle = null;

  @Before
  def prepare = {
    node = new ConsHashNode
    node.id = "node1";
    node.function = new DummyHashFunction;
    circle = new DummyCircle;
    node.circle = circle;
    jmsTemplate = new DummyJmsTemplate
    messageConverter = new JsonMessageConverter;
    var gson = new GsonMessageFactoryBean().getObject.asInstanceOf[Gson];
    messageConverter.translator = gson;
    jmsTemplate.setMessageConverter(messageConverter);
    node.jmsTemplate = jmsTemplate;
    node.afterPropertiesSet
  }

  @Test
  def testSimpleGet = {
    node.put("key1", "minority");
    assertEquals("minority", node.get("key1"));
    assertEquals(2, jmsTemplate.getStorage.get("ch.query").size);
    jmsTemplate.getStorage.get("ch.query").poll();
    jmsTemplate.getStorage.get("ch.query").poll();
    new Thread {
      {
        setName("Test-Thread");
        setDaemon(true);
      }
      override def run(): Unit = {
        Thread.sleep(200l);
        var query = messageConverter.fromMessage(
          jmsTemplate.getStorage.get("ch.query").poll())
          .asInstanceOf[QueryRequest];
        node.onQueryResult(new QueryResponse("node2", BigDecimal(8), "majority", query.sessionKey));
        node.onQueryResult(new QueryResponse("node3", BigDecimal(2), "majority", query.sessionKey));
      }
    }.start();
    assertEquals("majority", node.get("key1"));
    // local result should have been corrected
    assertEquals("majority", node.get("key1"));
  }

  @Test
  def testPut = {
    node.put("key", "value");
    assertEquals(2, jmsTemplate.getStorage.get("ch.set").size());
    var rawmsg1 = jmsTemplate.getStorage.get("ch.set").poll;
    var message1 = messageConverter.fromMessage(rawmsg1).asInstanceOf[SetRequest];
    var rawmsg2 = jmsTemplate.getStorage.get("ch.set").poll;
    var message2 = messageConverter.fromMessage(rawmsg2).asInstanceOf[SetRequest];

    assertEquals("node2", rawmsg1.getStringProperty("node_id"));
    assertEquals("node3", rawmsg2.getStringProperty("node_id"));

    assertEquals("key", message1.key);
    assertEquals("value", message1.value);
    assertEquals("node2", message1.nodeid);
    assertEquals(BigDecimal(2), message1.location);

    assertEquals("key", message2.key);
    assertEquals("value", message2.value);
    assertEquals("node3", message2.nodeid);
    assertEquals(BigDecimal(3), message2.location);
  }

  @Test
  def testOnSet = {
    try {
      node.onSet(new SetRequest("node1", BigDecimal(5), "key1", "value1"));
      fail();
    } catch {
      case e: IllegalArgumentException => {}
    }
    try {
      node.onSet(new SetRequest("node2", BigDecimal(4), "key2", "value2"));
      fail();
    } catch {
      case e: IllegalArgumentException => {}
    }
    node.onSet(new SetRequest("node1", BigDecimal(1), "key3", "value3"));

    assertEquals("value3", node.get("key3"));
    assertEquals(null, node.get("key1"));
    assertEquals(null, node.get("key2"));
  }

  @Test
  def testOnQuery = {
    node.onSet(new SetRequest("node1", BigDecimal(4), "key1", "value1"));
    var request = new QueryRequest("node1", BigDecimal(4), "key1", "session_key");
    node.onQuery(request);
    assertEquals(1, jmsTemplate.getStorage.get("ch.queryResp").size());
    var msg = messageConverter.fromMessage(jmsTemplate
      .getStorage.get("ch.queryResp").poll)
      .asInstanceOf[QueryResponse];
    assertEquals("session_key", msg.sessionKey);
    assertEquals("node1", msg.nodeid);
    assertEquals("value1", msg.result);
  }

  @Test
  def testOnStoreChange = {
    // Multiple add 
    node.onStoreChanged(new StoreAddMessage("node2"))
    node.onStoreChanged(new StoreAddMessage("node2"))
    var locs = circle.buffer.get("node2").get;
    assertTrue(locs.exists(_ == BigDecimal(3)));
    assertTrue(locs.exists(_ == BigDecimal(9)));
    assertTrue(locs.exists(_ == BigDecimal(8)));

    node.onStoreChanged(new StoreRemoveMessage("node2"));
    assertTrue(!circle.buffer.contains("node2"));

  }

  @Test
  def testJoin = {
    try {
      node.join
      fail();
    } catch {
      case e: RuntimeException => {}
    }
    assertEquals(1, jmsTemplate.getStorage.get("ch.sync").size());
    var msg = messageConverter
      .fromMessage(jmsTemplate.getStorage.get("ch.sync").poll)
      .asInstanceOf[SyncCircleRequest];
    assertEquals("node1", msg.id);

    new Thread() {
      override def run = {
        Thread.sleep(200l);
        node.onSyncCircleResponse(new SyncCircleResponse(Set[String]("node2", "node3")));
        node.onCopyResponse(new CopyResponse("node3", BigDecimal(2), BigDecimal(4)));
        node.onCopyResponse(new CopyResponse("node2", BigDecimal(3), BigDecimal(4)));

        node.onCopyResponse(new CopyResponse("node2", BigDecimal(9), BigDecimal(1)));
        node.onCopyResponse(new CopyResponse("node2", BigDecimal(8), BigDecimal(1)));

        node.onCopyResponse(new CopyResponse("node3", BigDecimal(5), BigDecimal(7)));
        //        node.onCopyResponse(new CopyResponse("node3", BigDecimal(6), BigDecimal(7)));
      }
    }.start();
    try {
      node.join
    } catch {
      case e: RuntimeException => {}
    }
    new Thread() {
      override def run = {
        Thread.sleep(200l);
        node.onSyncCircleResponse(new SyncCircleResponse(Set[String]("node2", "node3")));
        node.onCopyResponse(new CopyResponse("node3", BigDecimal(2), BigDecimal(4)));
        node.onCopyResponse(new CopyResponse("node2", BigDecimal(3), BigDecimal(4)));

        node.onCopyResponse(new CopyResponse("node2", BigDecimal(9), BigDecimal(1)));
        node.onCopyResponse(new CopyResponse("node2", BigDecimal(8), BigDecimal(1)));

        node.onCopyResponse(new CopyResponse("node3", BigDecimal(5), BigDecimal(7)));
        node.onCopyResponse(new CopyResponse("node3", BigDecimal(6), BigDecimal(7)));
      }
    }.start();
    node.join

    assertTrue(circle.buffer.contains("node1"));
  }

  @Test
  def testHeartbeat = {
    node.start
    Thread.sleep(4000l);
    assertTrue(jmsTemplate.getStorage.get("ch.heartbeat").size >= 4);

    var heartbeat = messageConverter
      .fromMessage(jmsTemplate.getStorage.get("ch.heartbeat").poll())
      .asInstanceOf[Heartbeat];
    assertEquals("node1", heartbeat.storeId);
  }

  @Test
  def testOnSyncCircle = {
    assertTrue(!circle.buffer.contains("node2"));
    assertTrue(!circle.buffer.contains("node3"));
    var resp = new SyncCircleResponse();
    resp.circle = Set[String]("node2", "node3");
    node.onSyncCircleResponse(resp);
    // Lock not set, this will not take effect
    assertTrue(!circle.buffer.contains("node2"));
    assertTrue(!circle.buffer.contains("node3"));
  }

  @Test
  def testOnSyncCircleRequest = {
    node.onSyncCircleRequest(new SyncCircleRequest());
    var msg = messageConverter
      .fromMessage(jmsTemplate.getStorage.get("ch.syncResp").poll)
      .asInstanceOf[SyncCircleResponse];
    assertTrue(msg.circle.contains("a"));
    assertTrue(msg.circle.contains("b"));
    assertTrue(msg.circle.contains("c"));
  }

  @Test
  def testLoad = {
    node.dataFile = "testdata/keyvalue";
    node.afterPropertiesSet();

    assertEquals("value001", node.get("key1"))
    assertEquals("value002", node.get("key2"))
    assertEquals("value003", node.get("key3"))
    assertEquals("value004", node.get("key4"))
  }
}

class DummyCircle extends HashCircle {

  var buffer = scala.collection.mutable.Map[String, Iterable[BigDecimal]]();

  def insert(locations: Iterable[BigDecimal], ref: String): Unit = {
    buffer += { ref -> locations };
  }

  def remove(ref: String): Iterable[(String, BigDecimal)] = {
    return buffer.remove(ref).getOrElse(Iterable.empty[BigDecimal]).map { x => (ref, x) };
  }

  def before(location: BigDecimal): Option[(String, BigDecimal)] = {
    // 1 2 3 4 5 6 7 8 9
    // 1 3 2 1 3 3 1 2 2
    return location match {
      case s if s == BigDecimal(1) => { Some("node2", BigDecimal(9)) }
      case s if s == BigDecimal(2) => { Some("node1", BigDecimal(1)) }
      case s if s == BigDecimal(3) => { Some("node3", BigDecimal(2)) }
      case s if s == BigDecimal(4) => { Some("node2", BigDecimal(3)) }
      case s if s == BigDecimal(5) => { Some("node1", BigDecimal(4)) }
      case s if s == BigDecimal(6) => { Some("node3", BigDecimal(5)) }
      case s if s == BigDecimal(7) => { Some("node3", BigDecimal(6)) }
      case s if s == BigDecimal(8) => { Some("node1", BigDecimal(7)) }
      case s if s == BigDecimal(9) => { Some("node2", BigDecimal(8)) }
      case _ => { None }
    }
  }

  def after(location: BigDecimal): Option[(String, BigDecimal)] = {
    return location match {
      case s if s == BigDecimal(1) => { Some("node3", BigDecimal(2)) }
      case s if s == BigDecimal(2) => { Some("node2", BigDecimal(3)) }
      case s if s == BigDecimal(3) => { Some("node1", BigDecimal(4)) }
      case s if s == BigDecimal(4) => { Some("node3", BigDecimal(5)) }
      case s if s == BigDecimal(5) => { Some("node3", BigDecimal(6)) }
      case s if s == BigDecimal(6) => { Some("node1", BigDecimal(7)) }
      case s if s == BigDecimal(7) => { Some("node2", BigDecimal(8)) }
      case s if s == BigDecimal(8) => { Some("node2", BigDecimal(9)) }
      case s if s == BigDecimal(9) => { Some("node1", BigDecimal(1)) }
      case _ => { None }
    }
  }

  def find(location: BigDecimal, size: Int): Iterable[(String, BigDecimal)] = {
    location match {
      case s if s == BigDecimal(0.4) => List(("node1", BigDecimal(1)), ("node2", BigDecimal(2)), ("node3", BigDecimal(3)));
      case s if s == BigDecimal(3.9) => List(("node1", BigDecimal(4)), ("node2", BigDecimal(2)), ("node3", BigDecimal(3)));
      case _ => List(("node1", BigDecimal(1)), ("node2", BigDecimal(2)), ("node3", BigDecimal(3)));
    }
  }

  def content: java.util.Set[String] = {
    return Set[String]("a", "b", "c");
  }
}

class DummyHashFunction extends HashFunction {
  def keyHash: (String) => BigDecimal = m => {
    m match {
      case "key1" => BigDecimal(0.5);
      case "key2" => BigDecimal(3.9);
      case "key3" => BigDecimal(0.5);
      case "key4" => BigDecimal(3.9);
      case _ => BigDecimal(0.5);
    }
  }

  def idDist: (String) => Iterable[BigDecimal] = {
    return m => {
      m match {
        case "node1" => List(BigDecimal(1), BigDecimal(4), BigDecimal(7));
        case "node2" => List(BigDecimal(3), BigDecimal(8), BigDecimal(9));
        case "node3" => List(BigDecimal(5), BigDecimal(2), BigDecimal(6));
      }
    }
  }
}