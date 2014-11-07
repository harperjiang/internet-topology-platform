package edu.clarkson.cs.scala.common.message

import java.util.ArrayList
import java.util.HashMap

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

import com.google.gson.Gson
import com.google.gson.GsonBuilder

class KVStoreTest {

  var gson: Gson = null;

  @Before
  def prepare = {
    var builder = new GsonBuilder();
    builder.registerTypeAdapter(classOf[KVStore], new KVStoreSD);

    gson = builder.create();
  }

  @Test
  def testSerialize = {
    var kvstore = new KVStore;
    kvstore.map.put("a", new Integer(5));
    kvstore.map.put("b", new java.lang.Long(23424324234324l));
    kvstore.map.put("c", "Good boy");

    var list = new ArrayList[Integer]();
    list.add(5);
    list.add(3);
    list.add(4);
    kvstore.map.put("intlist", list);

    var list2 = new ArrayList[java.util.Map[String, Object]]();
    list2.add(new HashMap[String, Object]());
    list2.add(new HashMap[String, Object]());

    list2.get(0).put("x", new Integer(345));
    list2.get(0).put("y", "datousuan");

    list2.get(1).put("w", new HashMap[String, Object]());
    list2.get(1).get("w").asInstanceOf[java.util.Map[String, Object]].put("inner", "Liu");

    kvstore.map.put("complexlist", list2);

    var res = "{\"a\":5,\"b\":23424324234324,\"c\":\"Good boy\",\"intlist\":[5,3,4],\"complexlist\":[{\"x\":345,\"y\":\"datousuan\"},{\"w\":{\"inner\":\"Liu\"}}]}"
    assertEquals(res, gson.toJson(kvstore).toString());
  }

  @Test
  def testDeserialize = {
    var res = "{\"a\":5,\"b\":23424324234324,\"c\":\"Good boy\",\"intlist\":[5,3,4],\"complexlist\":[{\"x\":345,\"y\":\"datousuan\"},{\"w\":{\"inner\":\"Liu\"}}]}"
    var kvstore = gson.fromJson(res, classOf[KVStore]);

    assertEquals(5.0, kvstore.map.get("a"))
    assertEquals(23424324234324.0, kvstore.map.get("b"));
    assertEquals("Good boy", kvstore.map.get("c"));
    var intlist = kvstore.map.get("intlist").asInstanceOf[java.util.List[_]];
    assertTrue(3 == intlist.size());
    assertEquals(5.0, intlist.get(0));
    assertEquals(3.0, intlist.get(1));
    assertEquals(4.0, intlist.get(2));

    var complexlist = kvstore.map.get("complexlist").asInstanceOf[java.util.List[Object]]

    assertEquals(345.0, complexlist.get(0).asInstanceOf[java.util.Map[String, Object]].get("x"))
    assertEquals("datousuan", complexlist.get(0).asInstanceOf[java.util.Map[String, Object]].get("y"))

    assertEquals("Liu", complexlist.get(1).asInstanceOf[java.util.Map[String, Object]].get("w").asInstanceOf[java.util.Map[String, Object]].get("inner"))
  }

}