package edu.clarkson.cs.itop.core.index

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.io.Source

import edu.clarkson.cs.common.io.LineCountingInputStream

class IndexSet private () {

  var folder: String = "";

  private val refLevel = 10;

  private val rootFile = "root"
  private val leafFile = "leaf";
  private val buffer = new ArrayBuffer[Buffer[IndexNode]](refLevel);

  var degree = 100;
  var fileLevel = 3;

  private var root = None: Option[IndexNode];
  private var leafCounter = 0;

  def this(fld: String) = {
    this();
    folder = fld;
    var fd = new File(folder);
    if (!fd.exists()) fd.mkdir();
    if (!fd.isDirectory()) throw new IllegalArgumentException("Destination is not a directory");
  }

  def this(fld: String, dge: Int) = {
    this(fld);
    degree = dge;
  }

  def range = (root.getOrElse(load).min, root.getOrElse(load).max)

  def find(target: Int): (Long, Long) = {
    var rootNode = root.getOrElse(load)
    rootNode find target;
  }

  private def load: IndexNode = {
    var ois = new ObjectInputStream(new FileInputStream(filename(rootFile)));
    var loaded = ois.readObject.asInstanceOf[IndexNode]
    ois.close
    loaded.refresh(this)
    root = Some(loaded)
    loaded
  }

  def build(input: String, filter: String => Boolean, parser: String => Int) = {
    // Initialize buffer structure
    buffer += new ArrayBuffer[IndexNode](degree);
    val cis = new LineCountingInputStream(new FileInputStream(input));
    var previousRecord = -1;
    var currentLeaf = newOffsetLeaf(degree);

    for (
      currentRecord <- Source.fromInputStream(cis).getLines
        .filter(line => {
          var res = filter(line); if (!res) cis.linestart; res
        })
        .map(parser)
    ) {
      if (currentRecord != previousRecord) {
        // Check current leaf and append
        var rec = (currentRecord, cis.linestart);
        if (currentLeaf.size == degree) {
          currentLeaf.updatelast(rec._2);
          buffer(0) += currentLeaf;
          merge(0, (size, lvl) => size >= degree)
          currentLeaf = newOffsetLeaf(degree);
        }
        currentLeaf.append(rec);
        previousRecord = currentRecord;
      }
    }
    if (currentLeaf.size != 0)
      buffer(0) += currentLeaf

    // Final cleanup and write root
    var curlevel = level;
    merge(0, (size, lvl) => size >= 1 && lvl < curlevel)
    curlevel = level
    if (buffer(curlevel).size > 1) {
      merge(curlevel, (size, lvl) => lvl == curlevel);
    }

    var oop = new ObjectOutputStream(new FileOutputStream(filename(rootFile)));
    var rootNode = buffer(level)(0);
    oop writeObject rootNode
    oop.close

    root = Some(rootNode)
  }

  private def merge(i: Int, workon: (Int, Int) => Boolean): Unit = {
    if (workon(buffer(i).size, i)) {
      var node = newIndexNode(buffer(i).size);
      node buildon buffer(i)
      buffer(i).clear
      bufferon(i + 1) += wrap(node, i + 1)
      merge(i + 1, workon)
    }
  }

  private def bufferon(index: Int): Buffer[IndexNode] = {
    while (buffer.size <= index) {
      buffer += new ArrayBuffer[IndexNode]()
    }
    buffer(index)
  }

  private def level: Int = {
    buffer.size - 1
  }

  private def wrap(node: IndexNode, lvl: Int): IndexNode = {
    if (lvl == fileLevel) {
      var currentLeaf = leafCounter;
      leafCounter += 1;
      newFileLeaf("%s_%d".format(leafFile, currentLeaf), node)
    } else
      node
  }

  private def newOffsetLeaf(dge: Int): OffsetLeaf = {
    var oleaf = new OffsetLeaf(dge);
    oleaf container = Some(this);
    oleaf
  }

  private def newIndexNode(dge: Int): IndexNode = {
    var inode = new IndexNode(dge);
    inode container = Some(this);
    inode
  }

  private def newFileLeaf(fn: String, n: IndexNode): FileLeaf = {
    new FileLeaf(fn, n);
  }

  protected[index] def filename(fn: String): String = "%s%s%s".format(folder, File.separator, fn);
}