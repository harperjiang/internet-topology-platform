package edu.clarkson.cs.itop.tool.reportdata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import org.mortbay.util.IO.NullWrite

object DegreenNumOfCluster extends App {
  var conf = new Configuration();

  var fs = FileSystem.get(conf);

  for (i <- 0 to 9) {
    iteration(i);
  }

  def iteration(index: Int) = {
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path(Config.file("report/degreen_clustersize_%d".format(index))), true);

    var job = Job.getInstance(conf, "Degree-n cluster Counter");
    job.setJarByClass(DegreenNumOfCluster.getClass());
    job.setMapperClass(classOf[ClusterCounterMapper]);
    job.setReducerClass(classOf[ClusterCounterReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[NullWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple_%d".format(index))))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("report/degreen_clustersize_%d".format(index))))
    job.waitForCompletion(true);
  }
}

class ClusterCounterMapper extends Mapper[Object, Text, IntWritable, NullWritable] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, NullWritable]#Context) = {
    var fields = value.toString().split("\\s+");
    context.write(new IntWritable(fields(1).toInt), NullWritable.get);
  }
}

class ClusterCounterReducer extends Reducer[IntWritable, NullWritable, IntWritable, Text] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[NullWritable],
    context: Reducer[IntWritable, NullWritable, IntWritable, Text]#Context) = {
    context.write(key, new Text(""));
  }
}