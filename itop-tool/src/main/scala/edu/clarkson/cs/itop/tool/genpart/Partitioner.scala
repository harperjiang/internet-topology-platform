package edu.clarkson.cs.itop.tool.genpart

import java.io.PrintWriter
import edu.clarkson.cs.itop.tool.Param
import java.io.FileOutputStream
import scala.io.Source

object Partitioner {

  def partitionLink: Unit = {
    var files = new Array[PrintWriter](Param.partition_count);

    for (i <- 0 to Param.partition_count - 1) {
      files(i) = new PrintWriter(new FileOutputStream("partitioned_link_%d".format(i)))
    }

    Source.fromFile("partition_link_input").getLines()
      .foreach(line =>
        {
          var split = line.indexOf("\t")
          var index = line.substring(0, split).trim.toInt
          files(index).println(line.substring(split))
        })

    for (i <- 0 to Param.partition_count - 1) {
      files(i).close()
    }
  }

  def partitionNode: Unit = {
    var files = new Array[PrintWriter](Param.partition_count);

    for (i <- 0 to Param.partition_count - 1) {
      files(i) = new PrintWriter(new FileOutputStream("partitioned_node_%d".format(i)))
    }

    Source.fromFile("partition_node_input").getLines()
      .foreach(line =>
        {
          var split = line.indexOf("\t")
          var index = line.substring(0, split).trim.toInt
          files(index).println(line.substring(split))
        })

    for (i <- 0 to Param.partition_count - 1) {
      files(i).close()
    }
  }
}