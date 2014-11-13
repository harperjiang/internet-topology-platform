package edu.clarkson.cs.itop.tool.partition

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.parser.Parser

object CountLinkDegree extends App {
var conf = new Configuration();
    var job = Job.getInstance(conf, "Count Link Degree");
    job.setJarByClass(CountLinkDegree.getClass);
    job.setMapperClass(classOf[CountLinkMapper]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(args(0)));
    FileOutputFormat.setOutputPath(job, new Path(args(1)));
    job.waitForCompletion(true);
}

class CountLinkMapper extends Mapper[Object, Text, Text, IntWritable] {
  var parser = new Parser();
  var newkey = new Text();

  def map(key: Object, value: Text, context: Context) = {
    if (!value.toString().startsWith("#")) {
      var link = parser.parse[Link](value.toString());
      newkey.set(link.id.toString());
      context.write(newkey, new IntWritable(link.nodeSize));
    }
  }
}
