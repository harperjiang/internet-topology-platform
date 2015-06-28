package edu.clarkson.cs.itop.tool.compare.avgdegree

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

object Main extends App {
  var conf = new Configuration();

  var fs = FileSystem.get(conf);
  // true stands for recursively deleting the folder you gave
  fs.delete(new Path(Config.file("compare/avgdegree")), true);
  call("geo");
  call("random");
  call("degreen");
  call("exp");

  def call(prefix: String) = {
    var job = Job.getInstance(conf, "Avg Degree Node Join - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AvgDegreeNodeJoinMapper]);
    job.setReducerClass(classOf[AvgDegreeNodeJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/node_partition".format(prefix))))
    FileInputFormat.addInputPath(job, new Path(Config.file("common/node_degree")))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgdegree/%s_raw".format(prefix))))
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Avg Degree Join");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AvgDegreeMapper]);
    job.setReducerClass(classOf[AvgDegreeReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgdegree/%s_raw".format(prefix))))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgdegree/%s".format(prefix))))
    job.waitForCompletion(true);
  }

}