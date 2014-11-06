package edu.clarkson.cs.itop.core.conhash

import scala.collection.mutable.ArrayBuffer

trait HashCircle {

  def insert(locations: Iterable[BigDecimal], ref: String): Unit;

  def remove(ref: String): Iterable[(String, BigDecimal)];

  def before(location: BigDecimal): (String, BigDecimal);

  def after(location: BigDecimal): (String, BigDecimal);

  def find(location: BigDecimal, size: Int): Iterable[(String, BigDecimal)];
}

class LinkedHashCircle extends HashCircle {

  var entry: LinkedNode = new LinkedNode;
  entry.next = entry;
  entry.previous = entry;
  entry.value = null;
  entry.location = new BigDecimal(java.math.BigDecimal.ZERO);

  def insert(locations: Iterable[BigDecimal], ref: String): Unit = {
    locations.foreach(loc => {
      var newNode = new LinkedNode;
      newNode.location = loc;
      newNode.value = ref;

      var start = entry;
      while (start != null) {
        if (start.location == newNode.location) {
          throw new IllegalArgumentException("Duplicate Entry");
        }
        if (start.location < newNode.location &&
          (start.next.location > newNode.location || start.next == entry)) {
          start.next.previous = newNode;
          newNode.next = start.next;
          start.next = newNode;
          newNode.previous = start;
          start = null;
        }
        start = start.next;
      }
    });
  }

  def remove(ref: String): Iterable[(String, BigDecimal)] = {
    var buffer = new ArrayBuffer[(String, BigDecimal)];
    var start = entry.next;
    while (start != null) {
      if (start == entry) {
        start = null;
      } else {
        if (start.value == ref) {
          start.previous.next = start.next;
          start.next.previous = start.previous;
          buffer += ((ref, start.location));
        }
        start = start.next;
      }
    }
    return buffer;
  }

  def before(location: BigDecimal): (String, BigDecimal) = {
    var nb = nodeBefore(location);
    if (null == nb)
      return null;
    return (nb.value, nb.location);
  }

  def after(location: BigDecimal): (String, BigDecimal) = {
    var nb = nodeBefore(location);
    if (null == nb)
      return null;
    var node = ignoreentry(nb.next);
    return (node.value, node.location);
  }

  def find(location: BigDecimal, size: Int): Iterable[(String, BigDecimal)] = {
    var nb = nodeBefore(location);
    var result = new ArrayBuffer[(String, BigDecimal)]();
    for (i <- 1 to size) {
      nb = ignoreentry(nb.next);
      result += ((nb.value, nb.location));
    }
    return result;
  }

  private def ignoreentry(node: LinkedNode): LinkedNode = {
    if (node == entry) return entry.next;
    return node;
  }

  private def nodeBefore(loc: BigDecimal): LinkedNode = {
    if (entry.next == entry) {
      return null;
    }
    if (loc < entry.next.location)
      return entry.previous;
    var start = entry.next
    while (start != entry) {
      if (start.location < loc && (start.next.location > loc || start.next == entry)) {
        return start;
      }
      start = start.next;
    }
    return null;
  }
}

class LinkedNode {
  var previous: LinkedNode = null;
  var next: LinkedNode = null;

  var location: BigDecimal = null;
  var value = "";
}