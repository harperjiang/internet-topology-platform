package edu.clarkson.cs.itop.mapreduce

import scala.io.Source
import java.io.FileOutputStream
import java.io.PrintWriter
import java.io.PrintWriter
import java_cup.parser
import java.io.File
import scala.io.Source
import scala.sys.process.stringToProcess
import edu.clarkson.cs.scala.common.HeapSorter

object Sorting {

  def sort(fileName: String, output: String)(comparator: (String, String) => Int): Unit = {
    val f = new File(fileName);
    val line = ("cat " + fileName) #| ("wc -l") !!
    val level = 0
    val split = Math.pow(2, level).toInt;

    if (level == 0) { // In memory sort
      heapSort(fileName, output, comparator);
    } else { // Merge sort
      val fileLine = Integer.parseInt(line trim) / split;
      val smallFiles = new Array[String](split);
      val smallSorted = new Array[String](split);
      var lines =
        for (line <- Source.fromFile(f).getLines) yield line;

      var count = 0;
      for (i <- 0 to split - 1) {
        smallFiles(i) = "%s.p%d".format(fileName, i);
        smallSorted(i) = "%s.ps0_%d".format(fileName, i);
        // Fill in the file
        var pw = new PrintWriter(smallFiles(i));

        if (i == split - 1) {
          lines foreach (pw println _)
        } else {
          for (k <- 0 to fileLine - 1)
            pw println lines.next
        }
        pw.close

        heapSort(smallFiles(i), smallSorted(i), comparator)
      }

      for (round <- 0 to level - 1)
        for (fi <- 0 to Math.pow(2, level - round).intValue - 1 by 2)
          mergeSort("%s.ps%d_%d".format(fileName, round, fi),
            "%s.ps%d_%d".format(fileName, round, fi + 1),
            "%s.ps%d_%d".format(fileName, round + 1, fi / 2), comparator);
      // Rename the merged file
      "mv %s.ps%d_%d %s".format(fileName, level, 0, output) !;

      // Delete intermediate files
      smallFiles foreach ("rm %s".format(_) !)
      // "rm %s.p*".format(fileName) !;
    }
  }

  private def mergeSort(fileA: String, fileB: String, output: String, comparator: (String, String) => Int) {
    var aline = Source.fromFile(fileA).getLines;
    var bline = Source.fromFile(fileB).getLines;

    var pw = new PrintWriter(new File(output));

    var a = None: Option[String];
    var b = None: Option[String];

    while (aline.hasNext && bline.hasNext) {
      if (a isEmpty) a = Some(aline.next);
      if (b isEmpty) b = Some(bline.next);
      comparator(a get, b get) match {
        case x if x <= 0 => {
          pw println (a.get)
          a = None;
        }
        case x if x >= 0 => {
          pw println (b.get)
          b = None;
        }
        case _ => {}
      }
    }
    if (!(a isEmpty)) {
      pw println (a get)
    }
    if (!(b isEmpty)) {
      pw println (b get)
    }
    if (aline.hasNext) {
      aline foreach (pw println _);
    } else {
      bline foreach (pw println _);
    }
    "rm %s".format(fileA) !;
    "rm %s".format(fileB) !;
    pw.close
  }

  private def heapSort(fileName: String, output: String, comparator: (String, String) => Int): Unit = {
    var dataList = Source.fromFile(fileName).getLines.toList

    var sortedList = new HeapSorter().sort(dataList, comparator);
    var pw = new PrintWriter(output);
    sortedList foreach (pw.println(_))
    pw close
  }

}