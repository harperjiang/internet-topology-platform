package edu.clarkson.cs.itop.tool.partition

import edu.clarkson.cs.itop.core.model.NodeLink
import edu.clarkson.cs.itop.core.mapreduce.Sorting
import edu.clarkson.cs.itop.core.tool.Config
import edu.clarkson.cs.itop.core.parser.Parser
import scala.collection.mutable.ArrayBuffer
import java.io.File
import edu.clarkson.cs.itop.core.mapreduce.Task

object OrderByNodeDegree extends App {

  var parser = new Parser();

  var reduce = new Function1[String, Iterator[String]] {

    var lastId = "";

    var degreeBuffer = new ArrayBuffer[Int](1);
    var nodelinkBuffer = new ArrayBuffer[NodeLink](2000);

    def apply(line: String): Iterator[String] = {
      if (line == Task.EOF) {
        return build;
      }
      var data = extract(line);
      var id = data._1;
      if (id != lastId) {
        var result: Iterator[String] = null;
        if (lastId != "") {
          result = build;
          process(data._2, data._3);
        }
        lastId = id;
        if (null == result)
          return Iterator.empty;
        return result;
      } else {
        process(data._2, data._3);
        return Iterator.empty;
      }
    }

    private def extract(line: String): (String, String, String) = {
      var split1 = line.splitAt(line.indexOf(" "));
      var part2 = split1._2.trim;
      var split2 = part2.splitAt(part2.indexOf(" "));
      (split1._1, split2._1, split2._2.trim)
    }

    def process(tp: String, content: String): Unit = {
      tp match {
        case "degree" => {
          degreeBuffer += content.split("\\s")(1).toInt
        }
        case "data" => {
          nodelinkBuffer += parser.parse[NodeLink](content);
        }
      }
    }

    def build: Iterator[String] = {
      var result = new ArrayBuffer[String]();

      for (degree <- degreeBuffer) {
        for (nodelink <- nodelinkBuffer) {
          result += "N%d L%d %d".format(nodelink.node, nodelink.link, degree)
        }
      }
      // Start a new buffer
      degreeBuffer.clear;
      nodelinkBuffer.clear;
      result.toIterator;
    }
  };

  println(System.currentTimeMillis)

  Task.execute(List(Config.file("nodelinks.degree"), Config.file("kapar-midar-iff.nodelinks")),
    Config.file("nodelink.joined"),
    (file, line) => {
      new File(file).getName() match {
        case "nodelinks.degree" => {
          var parts = line.split("\\s")
          (parts(0).substring(1), "%s %s".format("degree", line))
        }
        case "kapar-midar-iff.nodelinks" => {
          var nodelink = parser.parse[NodeLink](line)
          ("%d".format(nodelink.node), "%s %s".format("data", line))
        }
      }
    },
    reduce,
    (line1, line2) => {
      line1.splitAt(line1.indexOf(" "))._1 compare line2.splitAt(line2.indexOf(" "))._1
    });

  Sorting.sort(Config.file("nodelink.joined"), Config.file("nodelink.sorted"))((line1, line2) => {
    -(line1.split("\\s")(2).toInt compare line2.split("\\s")(2).toInt)
  });

  println(System.currentTimeMillis)
}