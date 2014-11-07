package edu.clarkson.cs.itop.core.conhash

import scala.collection.mutable.ArrayBuffer
import java.util.TreeMap
import java.util.HashMap
import scala.collection.JavaConversions._
import java.util.Comparator

trait HashCircle {

  def insert(locations: Iterable[BigDecimal], ref: String): Unit;

  def remove(ref: String): Iterable[(String, BigDecimal)];

  def before(location: BigDecimal): Option[(String, BigDecimal)];

  def afterOrOn(location: BigDecimal): Option[(String, BigDecimal)];

  def find(location: BigDecimal, size: Int): Iterable[(String, BigDecimal)];

  def toList: java.util.List[String];
}

class TreeMapHashCircle extends HashCircle {

  private val storage = new TreeMap[BigDecimal, String](new BigDecimalComparator);

  private val backref = new HashMap[String, Iterable[BigDecimal]];

  def insert(locations: Iterable[BigDecimal], ref: String): Unit = {
    if (backref.containsKey(ref))
      return ;
    locations.foreach(newloc => {
      if (storage.containsKey(newloc)) {
        throw new IllegalArgumentException("Duplicate key");
      }
      storage.put(newloc, ref);
    });
    backref.put(ref, locations);
  }

  def remove(ref: String): Iterable[(String, BigDecimal)] = {
    if (!backref.containsKey(ref))
      return Iterable.empty[(String, BigDecimal)];
    return backref.remove(ref).map(entry => {
      storage.remove(entry);
      (ref, entry)
    });
  }

  def before(location: BigDecimal): Option[(String, BigDecimal)] = {
    if (storage.isEmpty()) {
      return None;
    }
    var lowerEntry = storage.lowerEntry(location);
    if (lowerEntry == null)
      lowerEntry = storage.lastEntry();
    return Some(lowerEntry.getValue(), lowerEntry.getKey());
  }

  def afterOrOn(location: BigDecimal): Option[(String, BigDecimal)] = {
    if (storage.isEmpty()) {
      return None;
    }
    var higherEntry = storage.ceilingEntry(location);
    if (higherEntry == null)
      higherEntry = storage.firstEntry();
    return Some(higherEntry.getValue, higherEntry.getKey);
  }

  def find(location: BigDecimal, size: Int): Iterable[(String, BigDecimal)] = {
    if (storage.isEmpty()) {
      return Iterable.empty[(String, BigDecimal)];
    }
    var result = new scala.collection.mutable.HashSet[(String, BigDecimal)];
    var loc = location;
    for (i <- 1 to size) {
      var next = afterOrOn(loc).getOrElse(return result);
      loc = next._2;
      var oldsize = result.size;
      result += next;
      if (result.size == oldsize)
        // Not increasing, this means no more
        return result;
    }
    return result;
  }

  def toList: java.util.List[String] = {
    storage.values().toList
  }
}

class BigDecimalComparator extends Comparator[BigDecimal] {
  def compare(o1: BigDecimal, o2: BigDecimal): Int = {
    o1 - o2 match {
      case w if w > 0 => 1;
      case w if w == 0 => 0;
      case _ => -1;
    }
  }
}
