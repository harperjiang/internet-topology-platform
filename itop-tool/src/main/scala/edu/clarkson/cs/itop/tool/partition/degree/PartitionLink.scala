package edu.clarkson.cs.itop.tool.partition.degree

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.Param

/**
 * Input:   link_id node_id degree
 * Output:  link_id partition_id
 */

class PartitionLinkMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var data = value.toString().split("\\s");
    var node_id = data(1).toInt;
    var partition_id = Math.abs(node_id.hashCode()) % Param.partition_count;
    context.write(new IntWritable(data(0).toInt), new IntWritable(partition_id));
  }
}
