package edu.clarkson.cs.itop.tool

import java.io.FileInputStream
import java.io.ObjectInputStream

import edu.clarkson.cs.itop.model._

object AnalyzeIndex extends App {

  var ois = new ObjectInputStream(new FileInputStream("/home/harper/root"));
  var head = ois.readObject();
  var a = 1
}