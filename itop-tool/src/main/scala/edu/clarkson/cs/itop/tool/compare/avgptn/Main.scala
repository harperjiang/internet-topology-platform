package edu.clarkson.cs.itop.tool.compare.avgptn

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.partition.degree.JoinLinkDegreeMapper
import edu.clarkson.cs.itop.tool.partition.degree.JoinLinkDegreeReducer
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.partition.degree.Main1
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.io.Text

object Main extends App {
  var conf = new Configuration();

  FileSystem.get(conf).delete(new Path(Config.file("compare/avgptn")), true);

  call("geo");
  call("random");
  call("degree");

  def call(prefix: String) = {
    var job = Job.getInstance(conf, "Average Partition Per Vertex Cut - %s".format(prefix));
    job.setJarByClass(Main1.getClass);
    job.setMapperClass(classOf[AvgPartitionPerVertexCutMapper]);
    job.setReducerClass(classOf[AvgPartitionPerVertexCutReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/node_partition".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgptn/%s".format(prefix))));
    job.waitForCompletion(true);
  }

}