package edu.clarkson.cs.itop.core.index

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.ref.SoftReference
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.File

@SerialVersionUID(1L)
class FileLeaf(fn: String, r: (Int, Int)) extends IndexNode(1) {

  var file: String = fn;
  datas = null;
  vmin = r._1
  vmax = r._2
  @transient var node: SoftReference[IndexNode] = new SoftReference(null);

  def this(name: String, n: IndexNode) = {
    this(name, (n.min, n.max));
    this.container = n.container;
    var oop = new ObjectOutputStream(new FileOutputStream(filename(name)));
    oop writeObject n
    oop.close
    this.node = new SoftReference[IndexNode](n);
  }

  def getnode: SoftReference[IndexNode] = {
    if (null == this.node)
      this.node = new SoftReference(null);
    return this.node;
  }

  override def find(target: Int): (Long, Long) = {
    getnode.get match {
      case Some(next) => { next.find(target) }
      case None => { fetch; refresh(container.get); find(target); }
    }
  }

  override def refresh(ctn: IndexSet): Unit = {
    container = Some(ctn);
    getnode.get match {
      case Some(next) => { next.parent = Some(this); next.refresh(ctn); }
      case None => {}
    }
  }

  override def depth: Int = 1
  override def size: Int = 1

  protected def fetch = {
    var ois = new ObjectInputStream(new FileInputStream(filename(file)));
    var filenode: IndexNode = ois.readObject().asInstanceOf[IndexNode]
    ois.close
    node = new SoftReference[IndexNode](filenode);
    node
  }

  protected def filename(name: String): String = {
    container.getOrElse(throw new RuntimeException("No container assigned")).filename(name)
  }
}