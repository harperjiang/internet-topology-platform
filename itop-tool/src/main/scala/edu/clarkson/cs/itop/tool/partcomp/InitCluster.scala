package edu.clarkson.cs.itop.tool.partcomp

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

/**
 * Input: node_id, degree
 * Output: cluster_id, node_id
 */
class InitClusterNodeMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var parts = value.toString().split("\\s+");
    var v = new IntWritable(parts(0).toInt);
    context.write(v, v);
  }
}

/**
 * Input:  node_id, partition_id
 * Output: cluster_id 1 partition_ids
 */
class InitClusterInfoMapper extends Mapper[Object, Text, IntWritable, IntWritable] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {

  }
}

class InitClusterInfoReducer extends Reducer[IntWritable, IntWritable, IntWritable, Text] {
  override def reduce(key: IntWritable, value: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, Text]#Context) = {

  }
}
