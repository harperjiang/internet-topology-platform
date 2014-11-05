package edu.clarkson.cs.itop.core.tool

import java.io.File
import java.io.PrintWriter
import java.util.Comparator
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.seqAsJavaList
import scala.io.Source
import edu.clarkson.cs.itop.core.model.NodeLink
import edu.clarkson.cs.itop.core.parser.Parser
import edu.clarkson.cs.scala.common.HeapSorter


object PerformanceEval extends App {
  
  val parser = new Parser();
  
  val inputFileName = "/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/linknodes.p0";
  val outputFileName = "/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/linknodes.p0out";
  
  println(System.currentTimeMillis());
  heapSortJava(new File(inputFileName),new File(outputFileName));
  println(System.currentTimeMillis());
  
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