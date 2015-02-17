package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable

/**
 * Input: node_id, degree
 * Output: node_id, cluster_id(node_id)
 */
class PrepareClusterNodeMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var parts = value.toString().split("\\s+");
    var nodeId = new IntWritable(parts(0).toString().toInt)
    context.write(nodeId, nodeId);
  }
}
