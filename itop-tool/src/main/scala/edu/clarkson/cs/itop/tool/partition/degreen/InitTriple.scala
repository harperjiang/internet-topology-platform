package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

/**
 * Initialize the triple table with node_degree table
 * Input: node_degree (node_id, degree)
 * Output: node_id degree 0
 */
class InitTripleMapper extends Mapper[Object, Text, Text, Text] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context) = {
    var parts = value.toString().split("\\s+");
    context.write(new Text(parts(0)), new Text(Array(parts(1), "0").mkString("\t")));
  }
}
