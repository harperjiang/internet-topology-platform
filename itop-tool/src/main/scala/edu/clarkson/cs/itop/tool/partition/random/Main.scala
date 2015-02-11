package edu.clarkson.cs.itop.tool.partition.random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object Main extends App {
  var conf = new Configuration();
  var job = Job.getInstance(conf, "Random Partition");
  job.setJarByClass(Main.getClass);
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