package edu.clarkson.cs.itop.tool.reportdata

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import edu.clarkson.cs.itop.tool.common.CounterMapper
import edu.clarkson.cs.itop.tool.common.CounterReducer
import edu.clarkson.cs.itop.tool.common.CounterParam

object FoldClusterSize extends App {

  var conf = new Configuration();

  var fs = FileSystem.get(conf);

  for(i <- 0 to 7)
    call(i);
  
  def call(index: Int) = {
     // true stands for recursively deleting the folder you gave
    fs.delete(new Path(Config.file("report/fold_clustersize_%d".format(index))), true);

    var job = Job.getInstance(conf, "Fold cluster Counter");
    job.getConfiguration().set(CounterParam.KEY_INDEX, "1");
    job.setJarByClass(FoldClusterSize.getClass);
    job.setMapperClass(classOf[CounterMapper]);
    job.setReducerClass(classOf[CounterReducer]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/cluster_node_%d".format(index))))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("report/fold_clustersize_%d".format(index))))
    job.submit();
  }
}