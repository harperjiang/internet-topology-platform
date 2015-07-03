package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.io.Text
import org.apache.hadoop.yarn.webapp.Params
import edu.clarkson.cs.itop.tool.Param

object Main2 extends App {

  var conf = new Configuration();
  var fs = FileSystem.get(conf);

  for (i <- 1 to Param.degree_n - 1) {
    assignTriplePartition(i);
  }

  def assignTriplePartition(round: Int) = {
  fs.delete(new Path(Config.file("degree%d".format(round))), true);
    
    var job = Job.getInstance(conf, "Degree - Join Triple Link - %d".format(round));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[JoinTripleLinkMapper]);
    job.setReducerClass(classOf[JoinTripleLinkReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple_%d".format(round))));
    FileInputFormat.addInputPath(job, new Path(Config.file("common/node_link")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degree%d/triple_link_join".format(round))));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Degree - Assign Link to Partition - %d");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AssignLinkToPartitionMapper]);
    job.setReducerClass(classOf[AssignLinkToPartitionReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(Config.file("degree%d/triple_link_join".format(round))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degree%d/link_partition".format(round))));
    job.waitForCompletion(true);
  }
}