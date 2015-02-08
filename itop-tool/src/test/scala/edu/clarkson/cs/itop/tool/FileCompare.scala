package edu.clarkson.cs.itop.tool

import scala.io.Source

object FileCompare {

  def compare(input1: String, input2: String): Boolean = {
    var lines1 = Source.fromFile(input1).getLines();
    var lines2 = Source.fromFile(input2).getLines();

    lines1.foreach { line1 =>
      {
        var line2 = lines2.next();
        if (line1 != line2) {
          System.err.println("%s<==>%s".format(line1, line2));
          return false;
        }
      }
    };
    return true;
  }
}