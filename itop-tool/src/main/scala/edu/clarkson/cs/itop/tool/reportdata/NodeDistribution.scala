package edu.clarkson.cs.itop.tool.reportdata

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.common.CounterReducer
import edu.clarkson.cs.itop.tool.common.CounterMapper
import edu.clarkson.cs.itop.tool.common.CounterParam

object NodeDistribution extends App {

  var conf = new Configuration();

  call("random");
  call("degreen");
  call("exp");
  call("geo");

  def call(prefix:String) = {
    callSingle(prefix);
    callMulti(prefix);
  }
  
  def callSingle(prefix: String) = {

    FileSystem.get(conf).delete(new Path(Config.file("report/node_distribution_1_%s".format(prefix))), true);

    var job = Job.getInstance(conf, "Node Distribution 1 - %s".format(prefix));
    job.setJarByClass(NodeDistribution.getClass);
    job.setMapperClass(classOf[CounterMapper]);
    job.setReducerClass(classOf[CounterReducer]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.getConfiguration.set(CounterParam.KEY_INDEX, "1");
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/node_partition_raw".format(prefix))))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("report/node_distribution_1_%s".format(prefix))))
    job.submit();
  }

  def callMulti(prefix:String) = {
    FileSystem.get(conf).delete(new Path(Config.file("report/node_distribution_2_raw_%s".format(prefix))), true);
    FileSystem.get(conf).delete(new Path(Config.file("report/node_distribution_2_%s".format(prefix))), true);

    var job = Job.getInstance(conf, "Node Distribution 2-1 - %s".format(prefix));
    job.setJarByClass(NodeDistribution.getClass);
    job.setMapperClass(classOf[NodeDistributionMapper]);
    job.setReducerClass(classOf[MultiNodeDistributionReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/node_partition_raw".format(prefix))))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("report/node_distribution_2_raw_%s".format(prefix))))
    job.waitForCompletion(true);
    
    
    job = Job.getInstance(conf, "Node Distribution 2-2 - %s".format(prefix));
    job.getConfiguration.set(CounterParam.KEY_INDEX, "1");
    job.setJarByClass(NodeDistribution.getClass);
    job.setMapperClass(classOf[CounterMapper]);
    job.setReducerClass(classOf[CounterReducer]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(Config.file("report/node_distribution_2_raw_%s".format(prefix))))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("report/node_distribution_2_%s".format(prefix))))
    job.waitForCompletion(true);
  }
}

/**
 * Input: node_partition_raw (node_id, partition_id)
 */

class NodeDistributionMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var fields = value.toString().split("\\s+");
    context.write(new IntWritable(fields(0).toInt), new IntWritable(fields(1).toInt));
  }
}

/**
 * Output: how many partitions are crossed, number of nodes
 */
class MultiNodeDistributionReducer extends Reducer[IntWritable, IntWritable, IntWritable, IntWritable] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, IntWritable]#Context) = {
    var size = values.size;
    if (size > 1) {
      context.write(key, new IntWritable(size));
    }
  }
}