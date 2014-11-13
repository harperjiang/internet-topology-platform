package edu.clarkson.cs.itop.tool.partition

import scala.io.Source
import java.io.FileOutputStream
import java.io.PrintWriter
import edu.clarkson.cs.itop.core.model.NodeLink
import edu.clarkson.cs.itop.core.tool.Config
import edu.clarkson.cs.itop.core.parser.Parser

object CountNodeDegree extends App {

  var parser = new Parser();

  var nls = Source.fromFile(Config.file("kapar-midar-iff.nodelinks")).getLines().map(x => parser.parse[NodeLink](x))

  var pw = new PrintWriter(Config.file("nodelinks.degree"))

  var oldnode = -1;
  var counter = 0;
  for (nl <- nls) {
    if (nl.node != oldnode) {
      if (oldnode != -1) {
        pw.println("N%d\t%d".format(oldnode, counter))
      }
      oldnode = nl.node
      counter = 1
    } else {
      counter += 1
    }
  }
  pw.println("N%d\t%d".format(oldnode, counter))

  pw.close

}