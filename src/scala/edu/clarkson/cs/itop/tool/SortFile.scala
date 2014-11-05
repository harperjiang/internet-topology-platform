package edu.clarkson.cs.itop.tool

import scala.io.Source
import edu.clarkson.cs.itop.mapreduce.Sorting

object SortFile extends App {

  var inputFile = args(0);
  var outputFile = args(1);
  var column = args(2).toInt;
  var dataType = args(3).toInt; // 0 as String, 1 as Int
  var ascend = args(3).toInt; // 0 as asc, 1 as desc

  Sorting.sort(inputFile, outputFile)((line1, line2) => {
    var d1 = line1.split("\\s")(column);
    var d2 = line2.split("\\s")(column);
    var result = 0;
    dataType match {
      case 0 => {
        result = d1 compare d2;
      }
      case 1 => {
        result = d1.toInt compare d2.toInt;
      }
    }

    if (ascend == 1) {
      result = -result;
    }
    result;
  });
}