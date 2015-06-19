package edu.clarkson.cs.itop.tool.experiment

import scala.io.Source
import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.parser.Parser

/**
 * This Application generates a histogram for hashed result to show Java hash function is an iid.
 */
object HashExp extends App {

  var value = new Array[Array[Int]](12);
  for (i <- 0 to 11) {
    value(i) = Array(0, 0);
  }

  var parser = new Parser();

  var links = Source.fromFile("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/kapar-midar-iff.links")
    .getLines().filter(!_.startsWith("#")).map[Link] { parser.parse[Link](_) };

  links.foreach { link =>
    {
      var index = link.id.hashCode() % 12;
      value(index)(0) += 1;
      value(index)(1) += link.nodeSize;
    }
  };

  System.out.println(value.map { _.map(_.toString).mkString("\t") }.mkString("\n"));
}