package edu.clarkson.cs.itop.tool.compare.avgdegree

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

import edu.clarkson.cs.itop.tool.common.JoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper

/**
 * Input: node_partition (node_id, partition_id+)
 * Input: node_degree (node_id, degree)
 * Output: node_degree containing only nodes in routing table
 */

class AvgDegreeNodeJoinMapper extends SingleKeyJoinMapper("node_partition", "node_degree", 0, 0);

class AvgDegreeNodeJoinReducer extends JoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    (key, new Text(right(1).toString))
  });

/**
 * Input: node_degree (node_id, degree)
 * Output: average value
 */
class AvgDegreeMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  val statickey = new IntWritable(1);
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    context.write(statickey, new IntWritable(value.toString().split("\\s+")(1).toInt))
  }
}

class AvgDegreeReducer extends Reducer[IntWritable, IntWritable, IntWritable, Text] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, Text]#Context) = {
    var counter = 0;
    var sum = 0d;
    values.foreach { value => { counter += 1; sum += value.get } };
    context.write(new IntWritable(1), new Text((sum / counter).toString()));
  }
}