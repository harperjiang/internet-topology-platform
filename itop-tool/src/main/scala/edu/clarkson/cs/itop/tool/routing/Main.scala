package edu.clarkson.cs.itop.tool.routing

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

object Main extends App {
  var conf = new Configuration();

  var fs = FileSystem.get(conf);
  // true stands for recursively deleting the folder you gave
  fs.delete(new Path(Config.file("routing")), true);

  var job = Job.getInstance(conf, "Routing Node Machine");
  job.setMapperClass(classOf[NodeMachineMapper]);
  job.setReducerClass(classOf[NodeMachineReducer]);
  job.setMapOutputKeyClass(classOf[StringArrayWritable]);
  job.setMapOutputValueClass(classOf[StringArrayWritable]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[Text]);
  job.setPartitionerClass(classOf[KeyPartitioner]);
  job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
  FileInputFormat.addInputPath(job, new Path(Config.file("geo/link_partition")))
  FileInputFormat.addInputPath(job, new Path(Config.file("common/node_link")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("routing/routing")))
  job.waitForCompletion(true);
  
}