package edu.clarkson.cs.itop.model;

import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import edu.clarkson.cs.itop.model.Node;

class NodeTest extends FunSuite {
  test("Test Assign Node Values") {
    var node = new Node();
    node.ips = Set[String]("123");
  }
}