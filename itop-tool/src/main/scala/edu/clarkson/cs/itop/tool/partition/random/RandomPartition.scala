package edu.clarkson.cs.itop.tool.partition.random

import java.security.MessageDigest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.parser.Parser
import org.apache.hadoop.io.IntWritable

class RandomPartitionMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  var parser = new Parser();
  var digest = MessageDigest.getInstance("md5");
  val machineCount = 10;

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
    var line = value.toString();
    if (line.startsWith("#"))
      return ;
    var node = parser.parse[Node](line);
    context.write(new IntWritable(node.id), new IntWritable(hash(node.id)));
  }

  def hash(input: Int): Int = {
    new String(digest.digest(input.toString().getBytes)).hashCode() % machineCount;
  }
}
