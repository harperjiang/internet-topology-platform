package edu.clarkson.cs.itop.core.model

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.int2bigDecimal
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

  var namedLinks = SortedMap[String, Link]();
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

  def foreachLink(f: ((Link, LinkIndex)) => Unit) = {
    var index = new LinkIndex();
    index.onNamedLinks = true;
    index.nameKey = namedLinks.firstKey;
    var current = linkAtIndex(index);
    while (current != null) {
      f(current);
      current = nextLink(current._2);
    }
  }

  def linkAtIndex(index: LinkIndex): (Link, LinkIndex) = {
    var link: Link = null;
    if (index.onNamedLinks) {
      link = namedLinks.get(index.nameKey).get;
    } else if (index.anonymousIndex < anonymousLinks.length) {
      link = anonymousLinks(index.anonymousIndex);
    }
    if (link == null)
      return null;
    return (link, index);
  }

  def firstLink(): (Link, LinkIndex) = {
    var newIndex = new LinkIndex();
    if (namedLinks.isEmpty) {
      newIndex.onNamedLinks = false;
      newIndex.anonymousIndex = 0;
    } else {
      newIndex.onNamedLinks = true;
      newIndex.nameKey = namedLinks.firstKey;
    }
    return linkAtIndex(newIndex);
  }

  def nextLink(index: LinkIndex): (Link, LinkIndex) = {
    // Name nodes first, anonymous nodes then
    var newIndex = new LinkIndex(index);

    if (index.onNamedLinks) {
      var iterator = namedLinks.iteratorFrom(index.nameKey);
      // Ignore this one and return the "next" one
      iterator.next
      try {
        var expect = iterator.next;
        newIndex.nameKey = expect._1;
      } catch {
        case e: NoSuchElementException => {
          // No more named nodes, should switch to anonymous nodes
          newIndex.onNamedLinks = false;
          newIndex.anonymousIndex = 0;
        }
      }
    } else {
      newIndex.anonymousIndex += 1;
    }
    return linkAtIndex(newIndex);
  }
}

// A Link Index Uniquely identify a link connect to a node
class LinkIndex {
  var onNamedLinks = false;
  var nameKey = "";
  var anonymousIndex = 0;

  def this(copy: LinkIndex) = {
    this();
    this.onNamedLinks = copy.onNamedLinks;
    this.nameKey = copy.nameKey;
    this.anonymousIndex = copy.anonymousIndex;
  }
}