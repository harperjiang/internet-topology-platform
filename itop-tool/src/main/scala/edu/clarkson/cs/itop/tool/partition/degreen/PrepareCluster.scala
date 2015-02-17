package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

/**
 * Input: node_id degree
 * Output: node_id merge_times max_degree
 */
class PrepareClusterMapper extends Mapper[Object, Text, IntWritable, Text] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context) = {
    var parts = value.toString().split("\\s+");
    var nodeId = new IntWritable(parts(0).toString().toInt)
    var values = Array("0", parts(1).toString()).mkString("\t");
    context.write(nodeId, new Text(values));
  }
}
