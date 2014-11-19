package edu.clarkson.cs.itop.tool.count

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
  job.setJarByClass(CountNodeDegree.getClass);
  job.setMapperClass(classOf[CountNodeMapper]);
  job.setReducerClass(classOf[CountNodeReducer]);
  job.setMapOutputKeyClass(classOf[IntWritable]);
  job.setMapOutputValueClass(classOf[IntWritable]);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[IntWritable]);
  //  FileInputFormat.addInputPath(job, new Path(args(0)));
  //  FileOutputFormat.setOutputPath(job, new Path(args(1)));
  FileInputFormat.addInputPath(job, new Path("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/kapar-midar-iff.links"));
  FileOutputFormat.setOutputPath(job, new Path("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/output/node_degree"));
  job.waitForCompletion(true);
}

class CountNodeMapper extends Mapper[Object, Text, IntWritable, IntWritable] {
  var parser = new Parser();
  var one = new IntWritable(1);
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
    var line = value.toString();
    if (!line.startsWith("#")) {
      var nodelink = parser.parse[Link](line);
      nodelink.anonymousNodeIds.foreach { node_id =>
        {
          context.write(new IntWritable(node_id), one);
        }
      };
      nodelink.namedNodeIds.foreach(node_id => { context.write(new IntWritable(node_id._2), one) })
    }
  }
}

class CountNodeReducer extends Reducer[IntWritable, IntWritable, IntWritable, IntWritable] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, IntWritable]#Context): Unit = {
    var sum = 0;
    var iterator = values.iterator();
    while (iterator.hasNext()) {
      sum += iterator.next().get;
    }
    context.write(key, new IntWritable(sum));
  }
}