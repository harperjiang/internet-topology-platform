package edu.clarkson.cs.itop.tool

import org.apache.hadoop.mapreduce.lib.input.FileSplit

object Utils {
  def fileName(input: FileSplit): String = {
    if (input.getPath.getName.startsWith("part")) {
      return input.getPath.getParent.getName;
    }
    return input.getPath.getName;
  }
}