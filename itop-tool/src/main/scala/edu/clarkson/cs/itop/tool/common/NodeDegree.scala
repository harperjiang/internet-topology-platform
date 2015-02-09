package edu.clarkson.cs.itop.tool.common

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import edu.clarkson.cs.itop.core.parser.Parser
import edu.clarkson.cs.itop.core.model.Link

class NodeDegreeMapper extends Mapper[Object, Text, IntWritable, IntWritable] {
  var parser = new Parser();
  var one = new IntWritable(1);
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
    var line = value.toString();
    if (!line.startsWith("#")) {
      var nodelink = parser.parse[Link](line);
      nodelink.anonymousNodeIds.foreach { node_id =>
        {
          context.write(new IntWritable(node_id), one);
        }
      };
      nodelink.namedNodeIds.foreach(node_id => { context.write(new IntWritable(node_id._2), one) })
    }
  }
}

class NodeDegreeReducer extends Reducer[IntWritable, IntWritable, IntWritable, IntWritable] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, IntWritable]#Context): Unit = {
    var sum = 0;
    var iterator = values.iterator();
    while (iterator.hasNext()) {
      sum += iterator.next().get;
    }
    context.write(key, new IntWritable(sum));
  }
}