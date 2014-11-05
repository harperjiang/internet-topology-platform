package edu.clarkson.cs.itop.core.mapreduce

object Utils {

  def fetchKey(line: String): String = {
	line.splitAt(line.indexOf(" "))._1
  }
  
}