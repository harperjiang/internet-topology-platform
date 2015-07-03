package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.common.ConcatMapper
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.Param

object Main extends App {

  var conf = new Configuration();
  FileSystem.get(conf).delete(new Path(Config.file("degreen")), true);

  initTriple();
  for (i <- 0 to Param.degree_n - 1) {
    adjustTriple(i);
  }
  assignTriplePartition();

  def initTriple() = {
    var job = Job.getInstance(conf, "Degree - Init Triple");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[InitTripleMapper]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(Config.file("common/node_degree")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/triple")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Degree - Init Triple Retain");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[InitTripleRetainMapper]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/triple_retain")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Degree - Duplicate Adj Node");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AdjDuplicateMapper]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(Config.file("common/adj_node")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/adj_node_dup")));
    job.waitForCompletion(true);
  }

  def adjustTriple(round: Int) = {
    var fs = FileSystem.get(conf);

    generateNewTriple();

    fs.rename(new Path(Config.file("degreen/triple")), new Path(Config.file("degreen/triple_old")));

    var job = Job.getInstance(conf, "Degree - Triple Diff");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[TripleDiffMapper]);
    job.setReducerClass(classOf[TripleDiffReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(5);
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple_new")));
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple_old")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/triple")));
    job.waitForCompletion(true);

    generateNewTriple();

    fs.rename(new Path(Config.file("degreen/triple_old")), new Path(Config.file("degreen/triple_%d".format(round))));
    fs.delete(new Path(Config.file("degreen/triple")), true);
    fs.rename(new Path(Config.file("degreen/triple_new")), new Path(Config.file("degreen/triple")));
  }

  def generateNewTriple() = {
    var fs = FileSystem.get(conf);
    fs.delete(new Path(Config.file("degreen/adj_node_join")), true);
    fs.delete(new Path(Config.file("degreen/adj_node_join_diff")), true);
    fs.delete(new Path(Config.file("degreen/triple_new")), true);

    var job = Job.getInstance(conf, "Degree - Adj Tripple Join");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AdjTripleJoinMapper]);
    job.setReducerClass(classOf[AdjTripleJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setNumReduceTasks(5);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/adj_node_dup")));
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/adj_node_join_diff")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Degree-Concat");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[ConcatMapper]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[NullWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[NullWritable]);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/adj_node_join_diff")));
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple_retain")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/adj_node_join")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Degree - Triple Update");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[TripleUpdateMapper]);
    job.setReducerClass(classOf[TripleUpdateReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    job.setNumReduceTasks(5);
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/adj_node_join")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/triple_new")));
    job.waitForCompletion(true);
  }

  
  def assignTriplePartition() = {
    var job = Job.getInstance(conf, "Degree - Join Triple Link");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[JoinTripleLinkMapper]);
    job.setReducerClass(classOf[JoinTripleLinkReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple")));
    FileInputFormat.addInputPath(job, new Path(Config.file("common/node_link")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/triple_link_join")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Degree - Assign Link to Partition");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AssignLinkToPartitionMapper]);
    job.setReducerClass(classOf[AssignLinkToPartitionReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple_link_join")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/link_partition")));
    job.waitForCompletion(true);
  }
}