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

object Index {
  val beforeFirst: Index = new Index;
}

class IndexableIterator[T](namedItems: SortedMap[String, T], anonymousItems: List[T]) extends java.util.Iterator[T] {

  private var currentIndex = Index.beforeFirst;

  def isEmpty: Boolean = {
    return this.namedItems.isEmpty && this.anonymousItems.isEmpty;
  }

  def hasNext(): Boolean = {
    if (currentIndex == Index.beforeFirst) {
      return !isEmpty;
    }
    if (currentIndex.onNamedItems) {
      return currentIndex.nameKey != namedItems.lastKey || !anonymousItems.isEmpty
    } else {
      return currentIndex.anonymousIndex < anonymousItems.size - 1;
    }
  }

  def next(): T = {
    if (currentIndex == Index.beforeFirst) {
      currentIndex = new Index();
      if (!namedItems.isEmpty) {
        currentIndex.onNamedItems = true;
        currentIndex.nameKey = namedItems.firstKey;
      } else {
        currentIndex.onNamedItems = false;
        currentIndex.anonymousIndex = 0;
      }
    } else {
      // Name nodes first, anonymous nodes then
      if (currentIndex.onNamedItems) {
        if (index.nameKey.equals(namedItems.lastKey)) {
          // Already the last
          currentIndex.onNamedItems = false;
          currentIndex.anonymousIndex = 0;
        } else {
          var iterator = namedItems.iteratorFrom(index.nameKey);
          // Ignore this one and return the "next" one
          iterator.next
          var expect = iterator.next;
          currentIndex.nameKey = expect._1;
        }
      } else {
        currentIndex.anonymousIndex += 1;
      }
    }
    return at(currentIndex);
  }

  def remove: Unit = {
    throw new UnsupportedOperationException();
  }

  // Return Current Index
  def index(): Index = {
    currentIndex;
  }

  // Move the iterator to the element just after the given index
  def to(index: Index): T = {
    currentIndex = index;
    at(currentIndex);
  }

  private def at(index: Index): T = {
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

  def foreach(f: ((T, Index)) => Unit): Unit = {
    while (hasNext()) {
      f((next(), currentIndex));
    }
  }
}