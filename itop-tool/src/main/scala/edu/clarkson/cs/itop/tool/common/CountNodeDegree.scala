package edu.clarkson.cs.itop.tool.common

import scala.io.Source
import java.io.FileOutputStream
import java.io.PrintWriter
import edu.clarkson.cs.itop.core.model.NodeLink
import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.core.parser.Parser
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.core.model.Link

class CountNodeMapper extends Mapper[Object, Text, IntWritable, IntWritable] {
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

class CountNodeReducer extends Reducer[IntWritable, IntWritable, IntWritable, IntWritable] {
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