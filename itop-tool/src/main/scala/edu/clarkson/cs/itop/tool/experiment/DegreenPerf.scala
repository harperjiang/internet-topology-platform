package edu.clarkson.cs.itop.tool.experiment

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.partition.degree.NodeExpandMapper
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._
import edu.clarkson.cs.itop.tool.partition.degree.NodeExpandMapper
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

object Mainn extends App {

  var conf = new Configuration();

  var epjob = Job.getInstance(conf, "Degree n - Expand Node - Size Estimator");
  epjob.setJarByClass(Mainn.getClass);
  epjob.setMapperClass(classOf[NodeExpandMapper]);
  epjob.setReducerClass(classOf[SizeEstimateReducer]);
  epjob.setMapOutputKeyClass(classOf[StringArrayWritable]);
  epjob.setMapOutputValueClass(classOf[StringArrayWritable]);
  epjob.setOutputKeyClass(classOf[IntWritable]);
  epjob.setOutputValueClass(classOf[Text]);
  epjob.setPartitionerClass(classOf[KeyPartitioner]);
  epjob.setGroupingComparatorClass(classOf[KeyGroupComparator]);
  FileInputFormat.addInputPath(epjob, new Path(Config.file("degreen/node_expand_1")));
  FileInputFormat.addInputPath(epjob, new Path(Config.file("common/adj_node")));
  FileOutputFormat.setOutputPath(epjob, new Path(Config.file("degreen/node_expand_se")));
  epjob.waitForCompletion(true);

}

class SizeEstimateReducer extends Reducer[StringArrayWritable, StringArrayWritable, IntWritable, Text] {

  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, IntWritable, Text]#Context): Unit = {
    var nodeId = key.get()(0).toString().toInt
    var leftCounter = 0;
    var rightCounter = 0;
    values.foreach(value => {
      var group = value.get()(0).toString();
      group match {
        case "adj_node" => {
          leftCounter += 1;
        }
        case "node_expand" => {
          rightCounter += 1;
        }
      }
    });
    context.write(new IntWritable(nodeId), new Text(Array(leftCounter, rightCounter).map(_.toString).mkString("\t")))
  }
}