package edu.clarkson.cs.itop.core.tool.partition

import java.io.FileOutputStream
import java.io.PrintWriter
import scala.io.Source
import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.tool.Config
import edu.clarkson.cs.itop.core.parser.Parser


object CountLinkDegree extends App {

  var parser = new Parser();

  var nls = Source.fromFile(Config.dir + "kapar-midar-iff.links").getLines().filter(!_.startsWith("#")).map(x => parser.parse[Link](x))

  var pw = new PrintWriter(new FileOutputStream(Config.file("links.degree")))

  for (nl <- nls) {
    pw.println("L%d\t%d".format(nl.id, nl.nodeSize))
  }

  pw.close
}