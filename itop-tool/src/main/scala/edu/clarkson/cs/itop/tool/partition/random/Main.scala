package edu.clarkson.cs.itop.tool.partition.random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.fs.FileSystem

object Main extends App {
  var conf = new Configuration();

  FileSystem.get(conf).delete(new Path(Config.file("random")), true);

  var job = Job.getInstance(conf, "Random Partition");
  job.setJarByClass(Main.getClass);
  job.setMapperClass(classOf[RandomPartitionMapper]);
  job.setNumReduceTasks(0);
  FileInputFormat.addInputPath(job, new Path(Config.file("kapar-midar-iff.links")));
  FileOutputFormat.setOutputPath(job, new Path(Config.file("random/link_partition")));
  job.submit();
}