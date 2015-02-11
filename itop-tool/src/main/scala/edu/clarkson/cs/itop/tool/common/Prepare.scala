package edu.clarkson.cs.itop.tool.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.partition.degree.Main1
import edu.clarkson.cs.itop.tool.types.IntArrayWritable
import org.apache.hadoop.fs.FileSystem

object Prepare extends App {

  var conf = new Configuration();

  FileSystem.get(conf).delete(new Path(Config.file("common")), true);

  var job = Job.getInstance(conf, "Count Node Degree");
  job.setJarByClass(Prepare.getClass);
  job.setMapperClass(classOf[NodeDegreeMapper]);
  job.setReducerClass(classOf[NodeDegreeReducer]);
  job.setMapOutputKeyClass(classOf[IntWritable]);
  job.setMapOutputValueClass(classOf[IntWritable]);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(job, new Path(Config.file("kapar-midar-iff.links")));
  FileOutputFormat.setOutputPath(job, new Path(Config.file("common/node_degree")));
  job.submit();

  job = Job.getInstance(conf, "Adjacent Node");
  job.setJarByClass(Prepare.getClass);
  job.setMapperClass(classOf[AdjNodeMapper]);
  job.setReducerClass(classOf[AdjNodeReducer]);
  job.setMapOutputKeyClass(classOf[IntArrayWritable]);
  job.setMapOutputValueClass(classOf[IntWritable]);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(job, new Path(Config.file("kapar-midar-iff.links")));
  FileOutputFormat.setOutputPath(job, new Path(Config.file("common/adj_node")));
  job.submit();

  var nlmjob = Job.getInstance(conf, "Node Link Mapping");
  nlmjob.setJarByClass(Main1.getClass);
  nlmjob.setMapperClass(classOf[NodeToLinkMapper]);
  nlmjob.setOutputKeyClass(classOf[IntWritable]);
  nlmjob.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(nlmjob, new Path(Config.file("kapar-midar-iff.links")));
  FileOutputFormat.setOutputPath(nlmjob, new Path(Config.file("common/node_link")));
  nlmjob.submit();
}
