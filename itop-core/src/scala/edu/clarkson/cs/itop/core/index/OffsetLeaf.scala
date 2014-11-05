package edu.clarkson.cs.itop.core.index

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer

@SerialVersionUID(1L)
class OffsetLeaf(degree: Int) extends IndexNode(degree) {

  datas = null;
  var offsets: Buffer[(Int, Long, Long)] = new ArrayBuffer[(Int, Long, Long)](degree);

  def append(rec: (Int, Long)): Unit = {
    if (!offsets.isEmpty) {
      var last = offsets.last;
      offsets(offsets.size - 1) = (last._1, last._2, rec._2);
    }
    offsets += ((rec._1, rec._2, -1l));
  }

  def updatelast(next: Long) {
    if (!offsets.isEmpty) {
      var old = offsets.last;
      offsets(offsets.size - 1) = (old._1, old._2, next);
    }
  }

  override def min = offsets(0)._1;
  override def max = offsets.last._1;
  override def depth = 1

  override def find(target: Int): (Long, Long) = {
    var index = IndexUtils.bsearch[(Int, Long, Long)](offsets, target, (elem, target) => elem._1 - target);
    if (-1 == index) {
      return (-1l, 0l);
    }
    var elem = offsets(index);
    return (elem._2, elem._3);
  }

  override def refresh(ctn: IndexSet): Unit = {
    container = Some(ctn);
  }

  override def size: Int = {
    this.offsets size
  }
}