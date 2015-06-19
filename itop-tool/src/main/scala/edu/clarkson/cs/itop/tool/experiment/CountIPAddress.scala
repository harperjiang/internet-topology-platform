package edu.clarkson.cs.itop.tool.experiment

import edu.clarkson.cs.itop.core.parser.Parser
import scala.io.Source
import edu.clarkson.cs.itop.core.model.Node

object CountIPAddress extends App {
  
  var parser = new Parser();
  var counter = 0;
  Source.fromFile("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/kapar-midar-iff.nodes")
    .getLines().filter { !_.startsWith("#") }.foreach {
      line =>
        {
          var node = parser.parse[Node](line);
          counter += node.ips.size;
        }
    };
    System.out.println(counter);
}