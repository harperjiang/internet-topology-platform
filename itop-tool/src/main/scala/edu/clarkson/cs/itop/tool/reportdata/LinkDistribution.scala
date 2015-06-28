package edu.clarkson.cs.itop.tool.reportdata

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.common.SumMapper
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.common.SumReducer
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.conf.Configuration
import edu.clarkson.cs.itop.tool.common.SumParam
import edu.clarkson.cs.itop.tool.common.CounterParam
import edu.clarkson.cs.itop.tool.common.CounterReducer
import edu.clarkson.cs.itop.tool.common.CounterMapper

object LinkDistribution extends App {

  var conf = new Configuration();

  call("random");
  call("degreen");
  call("exp");
  call("geo");

  def call(prefix: String) = {
    var job = Job.getInstance(conf, "Link Distribution - %s".format(prefix));
    job.setJarByClass(LinkDistribution.getClass);
    job.setMapperClass(classOf[CounterMapper]);
    job.setReducerClass(classOf[CounterReducer]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    conf.set(CounterParam.KEY_INDEX, "0");
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/link_partition".format(prefix))))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("report/link_distribution_%s".format(prefix))))
    job.waitForCompletion(true);
  }

}

