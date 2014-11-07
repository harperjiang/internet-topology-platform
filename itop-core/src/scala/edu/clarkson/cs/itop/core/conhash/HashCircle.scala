package edu.clarkson.cs.itop.core.conhash

import scala.collection.mutable.ArrayBuffer
import java.util.TreeMap
import java.util.HashMap
import scala.collection.JavaConversions._

trait HashCircle {

  def insert(locations: Iterable[BigDecimal], ref: String): Unit;

  def remove(ref: String): Iterable[(String, BigDecimal)];

  def before(location: BigDecimal): (String, BigDecimal);

  def after(location: BigDecimal): (String, BigDecimal);

  def find(location: BigDecimal, size: Int): Iterable[(String, BigDecimal)];

  def toList: java.util.List[String];
}

class TreeMapHashCircle extends HashCircle {

  private val storage = new TreeMap[BigDecimal, String];

  private val backref = new HashMap[String, Iterable[BigDecimal]];

  def insert(locations: Iterable[BigDecimal], ref: String): Unit = {
    if (backref.containsKey(ref))
      return ;
    locations.foreach(storage.put(_, ref));
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

  def before(location: BigDecimal): (String, BigDecimal) = {
    var lowerEntry = storage.lowerEntry(location);
    if (lowerEntry == null)
      lowerEntry = storage.lastEntry();
    return (lowerEntry.getValue(), lowerEntry.getKey());
  }

  def after(location: BigDecimal): (String, BigDecimal) = {
    var higherEntry = storage.higherEntry(location);
    if (higherEntry == null)
      higherEntry = storage.firstEntry();
    return (higherEntry.getValue, higherEntry.getKey);
  }

  def find(location: BigDecimal, size: Int): Iterable[(String, BigDecimal)] = {
    var result = new ArrayBuffer[(String, BigDecimal)];
    var loc = location;
    for (i <- 1 to size) {
      var next = after(loc);
      loc = next._2;
      result += next;
    }
    return result;
  }

  def toList: java.util.List[String] = {
    storage.values().toList
  }
}
