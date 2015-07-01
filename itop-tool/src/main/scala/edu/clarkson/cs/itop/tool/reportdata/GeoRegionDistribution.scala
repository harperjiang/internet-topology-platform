package edu.clarkson.cs.itop.tool.reportdata

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.common.CounterReducer
import edu.clarkson.cs.itop.tool.common.CounterMapper
import edu.clarkson.cs.itop.tool.common.CounterParam
import org.apache.hadoop.io.Text

object GeoRegionDistribution extends App {

  var conf = new Configuration();
  var fs = FileSystem.get(conf);
  fs.delete(new Path(Config.file("report/geo_partition_dist")), true);

  var job = Job.getInstance(conf, "Geo Partition Dist");
  job.getConfiguration().set(CounterParam.KEY_INDEX, "1");
  job.setJarByClass(FoldClusterSize.getClass);
  job.setMapperClass(classOf[CounterMapper]);
  job.setReducerClass(classOf[CounterReducer]);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[Text]);
  FileInputFormat.addInputPath(job, new Path(Config.file("geo/geo_partition")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("report/geo_partition_dist")))
  job.submit();
}