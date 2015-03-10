package edu.clarkson.cs.itop.core.task.impl

import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.model.Index

class PathNode {
  // Latest node
  var node: Node = null;
  // The link connecting this node and its parent
  var link: Link = null;
  // The link's index in parent node
  var linkIndex: Index = null;
  // The node's index in link
  var nodeIndex: Index = null;
  def this(n: Node, l: Link, ni: Index, li: Index) = {
    this();
    node = n;
    link = l;
    nodeIndex = ni;
    linkIndex = li;
  }
}

class Path {

  var nodes = new java.util.ArrayList[PathNode];

  def length = nodes.size;

  def pop: PathNode = {
    if (nodes.isEmpty)
      return null;
    return nodes.remove(nodes.size - 1)
  };

  def push(node: PathNode): Unit = {
    nodes.add(node);
  }

  def top: PathNode = {
    if (nodes.isEmpty)
      return null;
    return nodes.get(nodes.size - 1)
  }

  def isEmpty: Boolean = nodes.isEmpty

  /**
   * Join two paths together
   */
  def join(another: Path): Unit = {
    if (another.length == 0)
      return ;
    if (length == 0) {
      this.nodes.addAll(another.nodes);
      return ;
    }
    if (another.nodes.get(0).node.id == nodes.get(length - 1).node.id) {
      another.nodes.remove(0);
    }
    this.nodes.addAll(another.nodes);
  }

  def clonePath(): Path = {
    var path = new Path();
    path.nodes.addAll(this.nodes);
    return path;
  }
}

class InfPath extends Path {
  override def length = Integer.MAX_VALUE;
}