package edu.clarkson.cs.itop.core.index

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer

/**
 * Each index node has a list of values and a list of nodes.
 *
 * If a value is between v_i(inclusive) and v_{i+1}(exclusive), then its node can be found in node_i
 */
@SerialVersionUID(1L)
class IndexNode(degree: Int) extends Serializable {

  var datas: Buffer[IndexNode] = new ArrayBuffer[IndexNode](degree);

  @transient var parent = None: Option[IndexNode];
  @transient var container = None: Option[IndexSet];

  var vmin = -1;
  var vmax = -1;

  def min: Int = {
    if (vmin == -1)
      vmin = datas(0).min
    vmin
  }
  def max: Int = {
    if (vmax == -1) {
      vmax = datas.last.max
    }
    vmax
  }

  def find(target: Int): (Long, Long) = {
    var index = IndexUtils.bsearch[IndexNode](datas, target, (data, target) => {
      if (data.min > target) 1;
      else if (data.max < target) -1;
      else 0;
    });
    if (index == -1)
      return (-1, 0);
    return datas(index).find(target);
  }

  def refresh(ctn: IndexSet): Unit = {
    container = Some(ctn);
    for (n <- datas) { n.parent = Some(this); n.refresh(ctn) };
  }

  def buildon(input: Buffer[IndexNode]): Unit = {
    datas ++= input;
    datas foreach (x => x.parent = Some(this))
  }

  def depth: Int = {
    if (datas.isEmpty)
      return 1;
    return datas(0).depth + 1
  }

  def size: Int = {
    this.datas size
  }
}