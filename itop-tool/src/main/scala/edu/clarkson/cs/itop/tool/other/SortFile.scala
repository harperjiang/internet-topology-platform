package edu.clarkson.cs.itop.tool.other

import scala.io.Source
import scala.collection.SortedMap
import scala.collection.JavaConversions._

object SortFile extends App {

  var file = Source.fromFile("/home/harper/Repositories/research-papers-hao/internet-latency/thesis/node_distribution_random_2.dat").getLines();
  var map = new java.util.TreeMap[Int, String]();
  file.foreach { line => { var parts = line.split("\\s+"); map.put(parts(0).toInt, parts(1)) } };
  map.foreach(f => { System.out.println("%d\t%s".format(f._1, f._2)) });
}