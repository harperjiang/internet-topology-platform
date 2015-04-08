package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.Param
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

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

    generateNewTriple();

    FileSystem.get(conf).rename(new Path(Config.file("degreen/triple")), new Path(Config.file("degreen/triple_old")));

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
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple_new")));
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple_old")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/triple")));
    job.waitForCompletion(true);

    FileSystem.get(conf).rename(new Path(Config.file("degreen/triple")), new Path(Config.file("degreen/triple_%d".format(round))));

    generateNewTriple();
  }

  def generateNewTriple() = {
    FileSystem.get(conf).delete(new Path(Config.file("degreen/adj_node_join")), true);
    FileSystem.get(conf).delete(new Path(Config.file("degreen/adj_node_join_diff")), true);

    var job = Job.getInstance(conf, "Degree - Adj Tripple Join");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AdjTripleJoinMapper]);
    job.setReducerClass(classOf[AdjTripleJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/adj_node_dup")));
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/triple")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/adj_node_join_diff")));
    job.waitForCompletion(true);

    FileSystem.get(conf).concat(new Path(Config.file("degreen/adj_node_join")),
      Array(new Path(Config.file("degreen/adj_node_join_diff")), new Path(Config.file("degreen/triple_retain"))));

    job = Job.getInstance(conf, "Degree - Triple Update");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[TripleUpdateMapper]);
    job.setReducerClass(classOf[TripleUpdateReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
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
    FileInputFormat.addInputPath(job, new Path(Config.file("degreen/node_link")));
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