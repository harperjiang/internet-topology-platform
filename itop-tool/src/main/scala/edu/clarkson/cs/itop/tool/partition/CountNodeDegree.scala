package edu.clarkson.cs.itop.tool.partition

import scala.io.Source
import java.io.FileOutputStream
import java.io.PrintWriter
import edu.clarkson.cs.itop.core.model.NodeLink
import edu.clarkson.cs.itop.core.tool.Config
import edu.clarkson.cs.itop.core.parser.Parser
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.core.model.Link

object CountNodeDegree extends App {

  var conf = new Configuration();
  var job = Job.getInstance(conf, "Count Node Degree");
  job.setJarByClass(CountLinkDegree.getClass);
  job.setMapperClass(classOf[CountLinkMapper]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(job, new Path(args(0)));
  FileOutputFormat.setOutputPath(job, new Path(args(1)));
  job.waitForCompletion(true);
}

class CountNodeMapper extends Mapper[Object, Text, Text, IntWritable] {
  var parser = new Parser();
  var keyText: Text = new Text();
  var one = new IntWritable(1);
  def map(key: Object, value: Text, context: Context) = {
    var line = value.toString();
    if (!line.startsWith("#")) {
      var nodelink = parser.parse[Link](line);
      nodelink.anonymousNodeIds.foreach { node_id =>
        {
          keyText.set(node_id.toString());
          context.write(keyText, one);
        }
      };
    }
  }
}

class CountNodeReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  def reduce(key: Text, values: java.util.Iterator[IntWritable], context: Context) = {
    var sum = 0;
    while (values.hasNext()) {
      sum += values.next().get;
    }
    context.write(key, new IntWritable(sum));
  }
}