package edu.clarkson.cs.itop.tool.partition.degree

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

object Main extends App {

  initCluster;
  mergeCluster;

  def initCluster = {
    var conf = new Configuration();
    var nlmjob = Job.getInstance(conf, "Node Link Mapping");
    nlmjob.setJarByClass(Main.getClass);
    nlmjob.setMapperClass(classOf[NodeToLinkMapper]);
    nlmjob.setOutputKeyClass(classOf[IntWritable]);
    nlmjob.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(nlmjob, new Path(Config.file("links")));
    FileOutputFormat.setOutputPath(nlmjob, new Path(Config.file("output/node_link")));
    nlmjob.waitForCompletion(true);

    conf = new Configuration();
    var icjob1 = Job.getInstance(conf, "Join Link Degree");
    icjob1.setJarByClass(Main.getClass);
    icjob1.setMapperClass(classOf[JoinLinkDegreeMapper]);
    icjob1.setReducerClass(classOf[JoinLinkDegreeReducer]);
    icjob1.setMapOutputKeyClass(classOf[StringArrayWritable]);
    icjob1.setMapOutputValueClass(classOf[StringArrayWritable]);
    icjob1.setOutputKeyClass(classOf[IntWritable]);
    icjob1.setOutputValueClass(classOf[Text]);
    icjob1.setPartitionerClass(classOf[KeyPartitioner]);
    icjob1.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(icjob1, new Path(Config.file("output/node_link")));
    FileInputFormat.addInputPath(icjob1, new Path(Config.file("output/node_degree")));
    FileOutputFormat.setOutputPath(icjob1, new Path(Config.file("output/init_cluster_1")));
    icjob1.waitForCompletion(true);

    conf = new Configuration();
    var icjob2 = Job.getInstance(conf, "Max Degree");
    icjob2.setJarByClass(Main.getClass);
    icjob2.setMapperClass(classOf[MaxDegreeMapper]);
    icjob2.setReducerClass(classOf[MaxDegreeReducer]);
    icjob2.setOutputKeyClass(classOf[IntWritable]);
    icjob2.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(icjob2, new Path(Config.file("output/init_cluster_1")))
    FileOutputFormat.setOutputPath(icjob2, new Path(Config.file("output/init_cluster_result")));
    icjob2.waitForCompletion(true);
  }

  def mergeCluster = {

  }
}

