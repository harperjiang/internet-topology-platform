package edu.clarkson.cs.itop.tool.compare.avgptn

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConversions._

/**
 * Input: routing_table (node_id partition_id+)
 * Output: a single number
 */

class AvgPartitionPerVertexCutMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  var statickey = new IntWritable(1);

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var parts = value.toString().split("\\s+")
    context.write(statickey, new IntWritable(parts.length - 1));
  }
}

class AvgPartitionPerVertexCutReducer extends Reducer[IntWritable, IntWritable, IntWritable, Text] {

  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, Text]#Context) = {
    var counter = 0;
    var sum = 0d;
    values.foreach { value => { counter += 1; sum += value.get; } };
    context.write(key, new Text(String.valueOf(sum / counter)));
  }
}