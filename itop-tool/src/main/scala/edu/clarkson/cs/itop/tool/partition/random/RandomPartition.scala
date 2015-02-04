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

object RandomPartition extends App {
  var conf = new Configuration();
  var job = Job.getInstance(conf, "Random Partition");
  job.setJarByClass(RandomPartition.getClass);
  job.setMapperClass(classOf[RandomPartitionMapper]);
  job.setMapOutputKeyClass(classOf[IntWritable]);
  job.setMapOutputValueClass(classOf[IntWritable]);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[IntWritable]);
  //  FileInputFormat.addInputPath(job, new Path(args(0)));
  FileInputFormat.addInputPath(job, new Path("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/kapar-midar-iff.nodes"))
  //  FileOutputFormat.setOutputPath(job, new Path(args(1)));
  FileOutputFormat.setOutputPath(job, new Path("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/output/random_partition"));

  job.waitForCompletion(true);
}

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
