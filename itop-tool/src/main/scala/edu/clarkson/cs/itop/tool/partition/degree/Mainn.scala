package edu.clarkson.cs.itop.tool.partition.degree

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.Param
import edu.clarkson.cs.itop.tool.common.DistinctMapper
import edu.clarkson.cs.itop.tool.common.DistinctReducer
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.common.MergeMapper
import org.apache.hadoop.io.NullWritable

object Mainn extends App {

  runJob;

  def runJob = {
    var conf = new Configuration();

    FileUtil.copy(FileSystem.get(conf), new Path(Config.file("common/adj_node")), FileSystem.get(conf), new Path(Config.file("degreen/node_expand_1")), false, true, conf);

    for (i <- 1 to Param.degree_n - 1) {
      var epjob = Job.getInstance(conf, "Degree n - Expand Node - %d".format(i));
      epjob.setJarByClass(Mainn.getClass);
      epjob.setMapperClass(classOf[NodeExpandMapper]);
      epjob.setReducerClass(classOf[NodeExpandReducer]);
      epjob.setMapOutputKeyClass(classOf[StringArrayWritable]);
      epjob.setMapOutputValueClass(classOf[StringArrayWritable]);
      epjob.setOutputKeyClass(classOf[IntWritable]);
      epjob.setOutputValueClass(classOf[IntWritable]);
      epjob.setPartitionerClass(classOf[KeyPartitioner]);
      epjob.setGroupingComparatorClass(classOf[KeyGroupComparator]);
      FileInputFormat.addInputPath(epjob, new Path(Config.file("degreen/node_expand_%d".format(i))));
      FileInputFormat.addInputPath(epjob, new Path(Config.file("common/adj_node")));
      FileOutputFormat.setOutputPath(epjob, new Path(Config.file("degreen/node_expand_%d".format(i + 1))));
      epjob.waitForCompletion(true);
    }

    var mergejob = Job.getInstance(conf, "Degree n - Merge node expansion");
    mergejob.setJarByClass(Mainn.getClass);
    mergejob.setMapperClass(classOf[MergeMapper]);
    mergejob.setOutputKeyClass(classOf[Text]);
    mergejob.setNumReduceTasks(0);
    mergejob.setOutputValueClass(classOf[NullWritable]);
    for (i <- 1 to Param.degree_n - 1) {
      FileInputFormat.addInputPath(mergejob, new Path(Config.file("degreen/node_expand_%d".format(i))));
    }
    FileOutputFormat.setOutputPath(mergejob, new Path(Config.file("degreen/node_expand_merge")));
    mergejob.waitForCompletion(true);

    var dstnctjob = Job.getInstance(conf, "Degree n - Distinct");
    dstnctjob.setJarByClass(Mainn.getClass);
    dstnctjob.setMapperClass(classOf[DistinctMapper]);
    dstnctjob.setReducerClass(classOf[DistinctReducer]);
    dstnctjob.setMapOutputKeyClass(classOf[StringArrayWritable]);
    dstnctjob.setMapOutputValueClass(classOf[Text]);
    dstnctjob.setOutputKeyClass(classOf[IntWritable]);
    dstnctjob.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(dstnctjob, new Path(Config.file("degreen/node_expand_merge")));
    FileOutputFormat.setOutputPath(dstnctjob, new Path(Config.file("degreen/node_expand")));
    dstnctjob.waitForCompletion(true);

    var icjob1 = Job.getInstance(conf, "Degree n - Join Link Degree");
    icjob1.setJarByClass(Mainn.getClass);
    icjob1.setMapperClass(classOf[JoinLinkDegreeMapper]);
    icjob1.setReducerClass(classOf[JoinLinkDegreeReducer]);
    icjob1.setMapOutputKeyClass(classOf[StringArrayWritable]);
    icjob1.setMapOutputValueClass(classOf[StringArrayWritable]);
    icjob1.setOutputKeyClass(classOf[IntWritable]);
    icjob1.setOutputValueClass(classOf[Text]);
    icjob1.setPartitionerClass(classOf[KeyPartitioner]);
    icjob1.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(icjob1, new Path(Config.file("degreen/node_expand")));
    FileInputFormat.addInputPath(icjob1, new Path(Config.file("common/node_degree")));
    FileOutputFormat.setOutputPath(icjob1, new Path(Config.file("degreen/link_degree")));
    icjob1.waitForCompletion(true);

    var icjob2 = Job.getInstance(conf, "Max Degree");
    icjob2.setJarByClass(Mainn.getClass);
    icjob2.setMapperClass(classOf[MaxDegreeMapper]);
    icjob2.setReducerClass(classOf[MaxDegreeReducer]);
    icjob2.setOutputKeyClass(classOf[IntWritable]);
    icjob2.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(icjob2, new Path(Config.file("degreen/link_degree")))
    FileOutputFormat.setOutputPath(icjob2, new Path(Config.file("degreen/max_degree")));
    icjob2.waitForCompletion(true);
  }

}

