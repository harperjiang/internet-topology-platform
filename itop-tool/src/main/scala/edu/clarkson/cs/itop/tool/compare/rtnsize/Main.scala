package edu.clarkson.cs.itop.tool.compare.rtnsize

import org.apache.hadoop.conf.Configuration
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
import edu.clarkson.cs.itop.tool.partition.degree.Main1

/**
 * Calculate the size of routing table
 */
object Main extends App {

  var conf = new Configuration();
  call("geo");
  call("random");
  call("degree");

  def call(prefix: String) = {

    conf.set(CounterParam.KEY_INDEX, "-1");

    var job = Job.getInstance(conf, "Size of Routing Table - %s".format(prefix));
    job.setJarByClass(Main1.getClass);
    job.setMapperClass(classOf[CounterMapper]);
    job.setReducerClass(classOf[CounterReducer]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/node_partition".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/rtnsize/%s".format(prefix))));
    job.waitForCompletion(true);
  }
}