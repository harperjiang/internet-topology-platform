package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable

/**
 * Initialize the triple table with node_degree table
 * Input: node_degree (node_id, degree)
 * Output: node_id core_id(node_id) degree 0
 */
class InitTripleMapper extends Mapper[Object, Text, IntWritable, Text] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context) = {
    var parts = value.toString().split("\\s+");
    context.write(new IntWritable(parts(0).toInt), new Text(Array(parts(0), parts(1), "0").mkString("\t")));
  }
}
/**
 * Input: triple(node_id, core_id, degree, 0)
 * Output: node_id node_id core_id degree 0
 */
class InitTripleRetainMapper extends Mapper[Object, Text, IntWritable, Text] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context) = {
    var parts = value.toString().split("\\s+");
    context.write(new IntWritable(parts(0).toInt), new Text(Array(parts(0), parts(1), parts(2), parts(3)).mkString("\t")));
  }
}
