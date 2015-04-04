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
import org.apache.hadoop.io.IntWritable

object Main extends App {
  var conf = new Configuration();

  var fs = FileSystem.get(conf);

  call("geo");
  call("random");
  call("degree");

  def call(prefix: String) = {
    var job = Job.getInstance(conf, "Node Partition Join - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[NodePartitionJoinMapper]);
    job.setReducerClass(classOf[NodePartitionJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/link_partition".format(prefix))))
    FileInputFormat.addInputPath(job, new Path(Config.file("common/node_link")))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/node_partition_raw".format(prefix))))
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Node Partition Filter - %s".format(prefix))
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[NodePartitionFilterMapper])
    job.setReducerClass(classOf[NodePartitionFilterReducer])
    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/node_partition_raw".format(prefix))))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/node_partition".format(prefix))))
    job.waitForCompletion(true);
  }
}