package edu.clarkson.cs.itop.tool.partition

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.partition.degree.NodeToLinkMapper
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.Utils
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import scala.collection.JavaConversions._
object TryGroupComparator extends App {

  var conf = new Configuration();
  var nlmjob = Job.getInstance(conf, "Try Group");
  nlmjob.setJarByClass(TryGroupComparator.getClass);
  nlmjob.setPartitionerClass(classOf[KeyPartitioner]);
  nlmjob.setGroupingComparatorClass(classOf[KeyGroupComparator]);
  nlmjob.setMapperClass(classOf[TryGroupMapper]);
  nlmjob.setReducerClass(classOf[TryGroupReducer]);
  nlmjob.setMapOutputKeyClass(classOf[StringArrayWritable]);
  nlmjob.setMapOutputValueClass(classOf[StringArrayWritable]);
  nlmjob.setOutputKeyClass(classOf[Text]);
  nlmjob.setOutputValueClass(classOf[Text]);
  FileInputFormat.addInputPath(nlmjob, new Path(Config.file("try_group_1")));
  FileInputFormat.addInputPath(nlmjob, new Path(Config.file("try_group_2")));
  FileOutputFormat.setOutputPath(nlmjob, new Path(Config.file("try_group_out")));
  nlmjob.waitForCompletion(true);
}

class TryGroupMapper extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {
  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context) = {
    var filename = Utils.fileName(context.getInputSplit.asInstanceOf[FileSplit]);
    var key = value.toString().split("\\s+");
    context.write(new StringArrayWritable(Array(key(0), filename)), new StringArrayWritable(Array(filename, key(1))));
  }
}

class TryGroupReducer extends Reducer[StringArrayWritable, StringArrayWritable, Text, Text] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, Text, Text]#Context) = {
    var result = 0;
    values.foreach(value => {
      value.get()(0).toString() match {
        case "try_group_1" => {result+=1;}
        case "try_group_2" => {result+=10;}
      }
    });
    context.write(new Text(key.get()(0).toString()),new Text(result.toString()))
  }
}