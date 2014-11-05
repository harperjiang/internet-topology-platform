package edu.clarkson.cs.itop.mapreduce

object Utils {

  def fetchKey(line: String): String = {
	line.splitAt(line.indexOf(" "))._1
  }
  
}