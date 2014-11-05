package edu.clarkson.cs.itop.mapreduce

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.io.Source

object Task {

  val EOF = "CAFEBABE";

  def execute(files: List[String], result: String,
    map: (String, String) => (String, String),
    reduce: String => Iterator[String],
    sort: (String, String) => Int = (line1, line2) => {
      Utils.fetchKey(line1) compare Utils.fetchKey(line2)
    }) = {
    var id = System currentTimeMillis;
    var mapname = "/tmp/%d.map".format(id)
    var sortname = "/tmp/%d.sort".format(id)
    var output = new PrintWriter(new FileOutputStream(mapname));

    // Map
    for (file <- files) {
      Source.fromFile(file).getLines foreach (line => {
        var result = map(file, line)
        output.println("%s %s".format(result._1, result._2))
      })
    }
    output.close

    // Sort
    Sorting.sort(mapname, sortname)(sort)

    // Reduce
    var finalout = new PrintWriter(result);
    Source.fromFile(sortname).getLines.map(reduce)
      .foreach(part => part foreach (finalout.println(_)));
    // This works as an eof
    reduce(EOF).foreach(finalout.println _);
    finalout.close();
  }
}

