package edu.clarkson.cs.itop.tool.compare.avgbc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import edu.clarkson.cs.itop.tool.Config

object Main extends App {

  var conf = new Configuration();
  var fs = FileSystem.get(conf);

  def run(prefix: String) = {
    fs.delete(new Path(Config.file("compare/avgbc/%s".format(prefix))), true);
  }

  def initCluster(prefix: String) = {
    var job = Job.getInstance(conf, "Avgbc - Init Cluster - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[InitClusterNodeMapper]);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/".format(prefix))))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/cluster_node".format(prefix))))
    job.waitForCompletion(true);
  }
}