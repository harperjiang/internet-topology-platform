package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.common.CounterMapper
import edu.clarkson.cs.itop.tool.common.CounterParam
import edu.clarkson.cs.itop.tool.common.CounterReducer

object Main extends App {

  var conf = new Configuration();
  
  FileSystem.get(conf).delete(new Path(Config.file("geo")), true);
  
  var job = Job.getInstance(conf, "Node Geo");
  job.setJarByClass(Main.getClass);
  job.setMapperClass(classOf[NodeGeoMapper]);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[Text]);
    job.setNumReduceTasks(0);
  FileInputFormat.addInputPath(job, new Path(Config.file("kapar-midar-iff.nodes.geo")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("geo/node_geo")))
  job.waitForCompletion(true);

  conf.set(CounterParam.KEY_INDEX, "1");
  job = Job.getInstance(conf, "Geo Count");
  job.setJarByClass(Main.getClass);
  job.setMapperClass(classOf[CounterMapper]);
  job.setReducerClass(classOf[CounterReducer]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(job, new Path(Config.file("geo/node_geo")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("geo/geo_count")))
  job.waitForCompletion(true);

}