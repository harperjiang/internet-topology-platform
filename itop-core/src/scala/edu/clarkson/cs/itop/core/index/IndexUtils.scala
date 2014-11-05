package edu.clarkson.cs.itop.core.index

import scala.collection.mutable.Buffer

object IndexUtils {

  def bsearch[A <: Any](buffer: Buffer[A], target: Int, comparator: (A, Int) => Int): Int = {
    return bsearch[A](buffer, target, 0, buffer.size - 1, comparator);
  }

  private def bsearch[A <: Any](buffer: Buffer[A], target: Int, from: Int, to: Int, comp: (A, Int) => Int): Int = {
    if (from >= to) {
      comp(buffer(from), target) match {
        case 0 => return from
        case _ => return -1
      }
    }

    var sep = (from + to) / 2
    comp(buffer(sep), target) match {
      case x if (x < 0) => bsearch(buffer, target, sep + 1, to, comp)
      case x if (x > 0) => bsearch(buffer, target, from, sep - 1, comp)
      case 0 => sep
    }
  }

}