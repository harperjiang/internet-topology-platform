package edu.clarkson.cs.scala.common

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class HeapSorter {

  var heapSize = 0;

  def sort[A <% AnyRef](input: List[A], comparator: (A, A) => Int): ArrayBuffer[A] = {
    var buffer = new ArrayBuffer[A](input.size);
    buffer addAll (input);
    heapSize = buffer.length;
    buildMaxHeap(buffer, comparator);
    for (i <- 1 to buffer.length - 1) {
      // Swap the heaptop and last one
      swap(buffer, 0, heapSize - 1);
      heapSize -= 1;
      maxHeapify(buffer, 0, comparator)
    }
    buffer
  }

  def buildMaxHeap[A <% AnyRef](buffer: ArrayBuffer[A], comparator: (A, A) => Int) = {
    for (i <- (heapSize / 2 - 1) to 0 by -1) {
      maxHeapify(buffer, i, comparator);
    }
  }

  def maxHeapify[A <% AnyRef](buffer: ArrayBuffer[A], i: Int, comparator: (A, A) => Int): Unit = {
    var me = i;
    var left = 2 * i + 1;
    var right = 2 * (i + 1);
    if (left >= heapSize)
      return
    var largest = comparator(buffer(me), buffer(left)) match {
      case x if x < 0 => { left }
      case _ => { me }
    }

    largest = if (right >= heapSize) largest else comparator(buffer(largest), buffer(right)) match {
      case x if x < 0 => { right }
      case _ => { largest }
    }

    if (largest != me) {
      swap(buffer, largest, me)
      maxHeapify(buffer, largest, comparator)
    }
  }

  def swap[A <% AnyRef](buffer: ArrayBuffer[A], a: Int, b: Int) = {
    var tmp = buffer(a);
    buffer(a) = buffer(b);
    buffer(b) = tmp;
  }
}