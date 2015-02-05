package edu.clarkson.cs.itop.tool.partition.degree

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import scala.collection.JavaConversions._

/**
 * Input:   node_id line_id degree
 * Output:  line_id node_id degree
 */

/**
 * Input:   node_id line_id degree
 * Output:  line_id node_id degree
 */
class MaxDegreeMapper extends Mapper[Object, Text, IntWritable, Text] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context) = {
    var data = value.toString().split("\\s");
    context.write(new IntWritable(data(1).toInt), new Text(Array(data(0), data(2)).mkString(" ")));
  }
}

/**
 * Input:   link_id node_id degree
 * Output:  link_id node_id degree (only the one with largest degree)
 */
class MaxDegreeReducer extends Reducer[IntWritable, Text, IntWritable, Text] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[Text],
    context: Reducer[IntWritable, Text, IntWritable, Text]#Context) = {
    var largest_degree = 0;
    var largest_node = -1;
    values.foreach(value => {
      var data = value.toString().split("\\s+");
      var current_degree = data(1).toInt;
      var current_node = data(0).toInt;
      if (current_degree > largest_degree) {
        largest_degree = current_degree;
        largest_node = current_node;
      }
    });
    context.write(key, new Text(Array(largest_node, largest_degree).map(_.toString).mkString("\t")))
  }
}