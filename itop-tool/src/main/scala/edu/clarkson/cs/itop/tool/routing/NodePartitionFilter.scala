package edu.clarkson.cs.itop.tool.routing

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper
import scala.collection.JavaConversions._
/**
 * Input: node_id partition_id
 * Output: node_id (partition_id)+ (nodes with more than one partitions)
 */

class NodePartitionFilterMapper extends Mapper[Object, Text, IntWritable, IntWritable] {
  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var parts = value.toString().split("\\s+")
    context.write(new IntWritable(parts(0).toString().toInt), new IntWritable(parts(1).toString().toInt))
  }

}

class NodePartitionFilterReducer extends Reducer[IntWritable, IntWritable, IntWritable, Text] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, Text]#Context) = {
    var valArray = scala.collection.mutable.ListBuffer[Int]();
    values.map { _.get }.foreach(valArray += _);
    if (valArray.length > 1) {
      context.write(key, new Text(valArray.mkString("\t")))
    }
  }
}