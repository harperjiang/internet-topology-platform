package edu.clarkson.cs.itop.tool.common

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.types.IntArrayWritable

object Prepare extends App {

  var conf = new Configuration();
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
  
  conf = new Configuration();
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
}
