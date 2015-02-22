package edu.clarkson.cs.itop.tool.genpart

import java.io.PrintWriter
import edu.clarkson.cs.itop.tool.Param
import java.io.FileOutputStream
import scala.io.Source
import java.io.File

object Partitioner {

  def partitionLink(input: File, outputFolder: File): Unit = {
    var files = new Array[PrintWriter](Param.partition_count);

    for (i <- 0 to Param.partition_count - 1) {
      files(i) = new PrintWriter(new FileOutputStream("%s%s%s".format(outputFolder.getAbsolutePath, File.separator, "partitioned_link_%d".format(i))))
    }

    Source.fromFile(input).getLines()
      .foreach(line =>
        {
          var split = line.indexOf("\t")
          if (split == -1) {
            throw new IllegalArgumentException(line);
          }
          var index = line.substring(0, split).trim.toInt
          files(index).println(line.substring(split + 1))
        })

    for (i <- 0 to Param.partition_count - 1) {
      files(i).close()
    }
  }

  def partitionNode(input: File, outputFolder: File): Unit = {
    var files = new Array[PrintWriter](Param.partition_count);

    for (i <- 0 to Param.partition_count - 1) {
      files(i) = new PrintWriter(new FileOutputStream("%s%s%s".format(outputFolder.getAbsolutePath, File.separator, "partitioned_node_%d".format(i))))
    }

    Source.fromFile(input).getLines()
      .foreach(line =>
        {
          var split = line.indexOf("\t")
          if (split == -1) {
            throw new IllegalArgumentException(line);
          }
          var index = line.substring(0, split).trim.toInt
          files(index).println(line.substring(split + 1))
        })

    for (i <- 0 to Param.partition_count - 1) {
      files(i).close()
    }
  }
}