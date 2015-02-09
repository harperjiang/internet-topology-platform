package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.Config

object Main extends App {

  var conf = new Configuration();
  var job = Job.getInstance(conf, "Geo Partition");
  job.setJarByClass(Main.getClass);
  job.setMapperClass(classOf[GeoPartitionMapper]);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(job, new Path(Config.file("kapar-midar-iff.nodes.geo")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("geo/geo_partition")))

  job.waitForCompletion(true);
}