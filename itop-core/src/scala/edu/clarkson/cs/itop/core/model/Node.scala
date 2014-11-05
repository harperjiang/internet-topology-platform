package edu.clarkson.cs.itop.core.model

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.int2bigDecimal
import scala.collection.mutable.Buffer
/**
 * A node represents a router
 */
class Node {

  val defaultIp = "";

  var id = 0;
  var asNum = 0;
  var asMethod = 0;
  var continent = "";
  var countryCode = "";
  var region = "";
  var city = "";
  var latitude: BigDecimal = 0;
  var longitude: BigDecimal = 0;

  var ips = Set[String]();

  var namedLinks = scala.collection.mutable.Map[String, Link]();
  var anonymousLinks = ArrayBuffer[Link]();

  def this(nid: Int) = {
    this();
    id = nid;
  }

  def this(sid: String, ipList: java.util.List[String]) = {
    this(Integer.parseInt(sid.substring(1)));
    ips = ipList.toSet
  }

  def appendLink(link: Link) = {
    anonymousLinks += link;
  }

  def appendLink(link: Link, ip: String) = {
    namedLinks += (ip -> link);
  }
}