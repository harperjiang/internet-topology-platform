package edu.clarkson.cs.itop.tool

import java.io.File
import java.io.PrintWriter
import scala.io.Source
import scala.sys.process.stringToProcess
import scala.collection.JavaConversions._
import edu.clarkson.cs.itop.model.NodeLink
import edu.clarkson.cs.itop.parser.Parser
import java.util.Comparator
import java.text.MessageFormat
import scala.collection.mutable.ListBuffer
import edu.clarkson.cs.scala.common.HeapSorter

/**
 * Link data is originally in the format of
 * link <link_id> (<node_id>(:<ip>)?[ ])+
 *
 * This tool convert it into the format of
 * nodelinks <node_id>(:<ip>)? : (<link_id>[ ])+
 */
object ConvertLinkFileFormat extends App {

  val linkFileName = "/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/kapar-midar-iff.links";
  val parser = new Parser();
  val destFileName = new File(linkFileName).getParent() + File.separator + "linknodes";
  /*
  val printer = new PrintWriter(destFileName);
  for (line <- Source.fromFile(linkFileName).getLines map (l => l.trim()) if !line.startsWith("#")) {
    var link: Link = parser.parse(line);
    for (n <- link.nodes) {
      var sb = new StringBuilder();
      sb append "nodelink " append "N" append n.node
      if (!n.ip.isEmpty) {
        sb append ":" append n.ip.get;
      }
      sb append ":"
      sb append "L" append link.id
      printer.println(sb toString);

    }
  }

  printer close
*/
  mergeSort(destFileName)((a:String, b:String) => {
    var nl1: NodeLink = parser.parse(a)
    var nl2: NodeLink = parser.parse(b)
    if(nl1 == null) {
      println(a)
    }
    if(nl2 == null) {
      println(b)
    }
    nl1.node compare nl2.node
  })

  def mergeSort(fileName: String)(comparator: (String, String) => Int): Unit = {
    val f = new File(fileName);
    val line = ("cat " + fileName) #| ("wc -l") !!
    val split = 32;
    val fileLine = Integer.parseInt(line trim) / split;
    val smallFiles = new Array[File](split);
    val smallSorted = new Array[File](split);

    var lines =
      for (line <- Source.fromFile(f).getLines) yield line;

    var count = 0;
    for (i <- 0 to split - 1) {
      smallFiles(i) = new File(fileName + ".p" + i);
      smallSorted(i) = new File(fileName + ".s0_" + i);
      // Fill in the file
      var pw = new PrintWriter(smallFiles(i));

      if(i== split -1) {
        lines foreach (pw println _)
      } else {
    	  for (k <- 0 to fileLine-1)
    		  pw println lines.next
      }
      pw.close

      heapSort(smallFiles(i), smallSorted(i))
    }
    for (round <- 0 to 4)
      for (fi <- 0 to Math.pow(2, 5 - round).intValue - 1 by 2)
        mergeSort(fileName + ".s" + round + "_" + fi,
          fileName + ".s" + round + "_" + (fi + 1),
          fileName + ".s" + (round + 1) + "_" + (fi / 2), comparator);
  }

  def mergeSort(fileA: String, fileB: String, output: String, comparator: (String, String) => Int) {
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
    if(!(a isEmpty)) {
      pw println (a get)
    }
    if(!(b isEmpty)) {
      pw println (b get)
    }
    if (aline.hasNext) {
      aline foreach (pw println _);
    } else {
      bline foreach (pw println _);
    }

    pw.close
  }

  def heapSort(fileName: File, output: File): Unit = {
    var fileList = Source.fromFile(fileName).getLines
    var dataList: List[NodeLink] = fileList.map[NodeLink](x => parser parse[NodeLink] x) toList;
    
    var sortedList: java.util.List[NodeLink] = new HeapSorter().sort(dataList, 
        (a: NodeLink, b: NodeLink)=> {a.node compare b.node
    });
    var pw = new PrintWriter(output);
    sortedList foreach (x => 
      if(x.ip.isEmpty) 
        pw println("nodelink N%d:L%d".format(x.node,x.link))
      else 
        pw println("nodelink N%d:%s:L%d".format(x.node,x.ip,x.link)));

    pw close
  }
  
  def heapSortJava(fileName: File, output: File): Unit = {
    var fileList = Source.fromFile(fileName).getLines
    var dataList: List[NodeLink] = fileList.map[NodeLink](x => parser parse[NodeLink] x) toList;
    
    var sortedList: java.util.List[NodeLink] = new edu.clarkson.cs.common.sort.HeapSorter(false).sort(dataList, 
        new Comparator[NodeLink] {
    	def compare(a: NodeLink, b: NodeLink)= {a.node compare b.node}
    });
    var pw = new PrintWriter(output);
    sortedList foreach (x => 
      if(x.ip.isEmpty) 
        pw println("nodelink N%d:L%d".format(x.node,x.link))
      else 
        pw println("nodelink N%d:%s:L%d".format(x.node,x.ip,x.link)));

    pw close
  }

}