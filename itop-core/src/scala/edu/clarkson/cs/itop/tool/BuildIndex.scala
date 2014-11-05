package edu.clarkson.cs.itop.tool

import edu.clarkson.cs.itop.index.IndexSet
import edu.clarkson.cs.itop.parser.Parser
import edu.clarkson.cs.itop.model.NodeLink
import edu.clarkson.cs.itop.model.Node


object BuildIndex extends App {
  var degree = 50
  var fileLevel = 2
  if (args.length >= 2) {
    degree = args(0).toInt
    fileLevel = args(1).toInt
  }
  var parser = new Parser();
  val ib = new IndexSet("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/nodelinks.index");
  ib.degree = degree
  ib.fileLevel = fileLevel
  ib.build("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/kapar-midar-iff.nodelinks",
    line => { !line.startsWith("#") },
    input => { var nl = parser.parse(input).asInstanceOf[NodeLink]; nl.node });
  val ib2 = new IndexSet("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/nodes.index");
  ib2.degree = degree
  ib2.fileLevel = fileLevel
  ib2.build("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/kapar-midar-iff.nodes",
    line => { !line.startsWith("#") },
    input => { var nl = parser.parse(input).asInstanceOf[Node]; nl.id });

}