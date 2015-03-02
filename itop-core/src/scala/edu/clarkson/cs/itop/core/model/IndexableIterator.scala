package edu.clarkson.cs.itop.core.model

import scala.collection.mutable.Buffer
import scala.collection.SortedMap

class Index {
  var onNamedItems = false;
  var nameKey = "";
  var anonymousIndex = 0;

  def this(copy: Index) = {
    this();
    this.onNamedItems = copy.onNamedItems;
    this.nameKey = copy.nameKey;
    this.anonymousIndex = copy.anonymousIndex;
  }
}

class IndexableIterator[T](namedItems: SortedMap[String, T], anonymousItems: List[T]) {

  def foreach(f: ((T, Index)) => Unit): Unit = {
    try {
      var index = first._2;
      var item = at(index);

      while (true) {
        f((item, index));
        var tuple = next(index);
        if (tuple == null)
          return ;
        item = tuple._1;
        index = tuple._2;
      }
    } catch {
      case e: NoSuchElementException => {
        // No more, stop
      }
    }
  }

  def at(index: Index): T = {
    try {
      if (index.onNamedItems) {
        return namedItems.get(index.nameKey).get;
      } else {
        return anonymousItems(index.anonymousIndex);
      }
    } catch {
      case e: NoSuchElementException => {
        throw e;
      }
      case e: IndexOutOfBoundsException => {
        throw new NoSuchElementException();
      }
    }
  }

  def first: (T, Index) = {
    var newIndex = new Index();
    if (namedItems.isEmpty) {
      newIndex.onNamedItems = false;
      newIndex.anonymousIndex = 0;
    } else {
      newIndex.onNamedItems = true;
      newIndex.nameKey = namedItems.firstKey;
    }
    return (at(newIndex), newIndex);
  }

  def last: (T, Index) = {
    var newIndex = new Index();
    if (anonymousItems.isEmpty) {
      newIndex.onNamedItems = true;
      newIndex.nameKey = namedItems.lastKey;
    } else {
      newIndex.onNamedItems = false;
      newIndex.anonymousIndex = anonymousItems.length - 1;
    }
    return (at(newIndex), newIndex);
  }

  def isEmpty: Boolean = {
    return this.namedItems.isEmpty && this.anonymousItems.isEmpty;
  }

  def next(index: Index): (T, Index) = {
    // Name nodes first, anonymous nodes then
    var newIndex = new Index(index);
    if (index.onNamedItems) {
      if (index.nameKey.equals(namedItems.lastKey)) {
        // Already the last
        newIndex.onNamedItems = false;
        newIndex.anonymousIndex = 0;
      } else {
        var iterator = namedItems.iteratorFrom(index.nameKey);
        // Ignore this one and return the "next" one
        iterator.next
        var expect = iterator.next;
        newIndex.nameKey = expect._1;
      }
    } else {
      newIndex.anonymousIndex += 1;
    }
    return (at(newIndex), newIndex);
  }
}