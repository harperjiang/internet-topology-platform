package edu.clarkson.cs.itop.tool.compare.avgbc

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
import edu.clarkson.cs.itop.tool.common.DistinctMapper
import edu.clarkson.cs.itop.tool.common.DistinctReducer
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.NullWritable

object Main extends App {

  var conf = new Configuration();
  var fs = FileSystem.get(conf);
  run("geo");

  def run(prefix: String) = {
    var baseFolder = "compare/avgbc/%s".format(prefix);

    fs.delete(new Path(Config.file(baseFolder)), true);

    initCluster(prefix, baseFolder);
    var emd = false;
    while (!emd) {
      rawMergeDecision(prefix, baseFolder);
      emd = emptyMergeDecision(baseFolder);
      if (!emd) {
        refineMergeDecision(prefix, baseFolder);
        updateClusters(prefix, baseFolder);
      }
    }
  }

  def initCluster(prefix: String, base: String) = {
    var job = Job.getInstance(conf, "Avgbc - Init Cluster - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[InitClusterNodeMapper]);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(Config.file("common/node_degree")))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/cluster_node".format(base))))
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Avgbc - Init Cluster Info - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[InitClusterInfoMapper]);
    job.setReducerClass(classOf[InitClusterInfoReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/node_partition_raw".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/cluster_info".format(base))))
    job.waitForCompletion(true);

    FileUtil.copy(FileSystem.get(conf), new Path(Config.file("common/adj_node")), FileSystem.get(conf),
      new Path(Config.file("%s/adj_cluster".format(base))), false, true, conf);
  }

  def rawMergeDecision(prefix: String, base: String) = {

    fs.delete(new Path(Config.file("%s/adj_cluster_left".format(base))), true);

    fs.delete(new Path(Config.file("%s/merge_decision_raw".format(base))), true);

    var job = Job.getInstance(conf, "Raw Merge Left - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AdjClusterLeftJoinMapper]);
    job.setReducerClass(classOf[AdjClusterLeftJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/cluster_info".format(base))));
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/adj_cluster".format(base))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/adj_cluster_left".format(base))));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Raw Merge Right - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AdjClusterRightJoinMapper]);
    job.setReducerClass(classOf[AdjClusterRightJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/adj_cluster_left".format(base))));
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/cluster_info".format(base))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/merge_decision_raw".format(base))))
    job.waitForCompletion(true);

    //     job = Job.getInstance(conf, "Raw Merge Distinct - %s".format(prefix));
    //    job.setJarByClass(Main.getClass);
    //    job.setMapperClass(classOf[DistinctMapper]);
    //    job.setReducerClass(classOf[DistinctReducer]);
    //    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    //    job.setMapOutputValueClass(classOf[Text]);
    //    job.setOutputKeyClass(classOf[Text]);
    //    job.setOutputValueClass(classOf[NullWritable]);
    //    job.setNumReduceTasks(6);
    //    FileInputFormat.addInputPath(job, new Path(Config.file("%s/merge_decision_dup".format(base))));
    //    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/merge_decision_raw".format(base))));
    //    job.waitForCompletion(true);
  }

  def refineMergeDecision(prefix: String, base: String) = {
    fs.delete(new Path(Config.file("%s/merge_decision_refine_th".format(base))), true);
    fs.delete(new Path(Config.file("%s/merge_decision".format(base))), true);

    var job = Job.getInstance(conf, "Avgbc - Refine Two Head - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[RemoveTwoHeadMergeDecisionMapper]);
    job.setReducerClass(classOf[RemoveTwoHeadMergeDecisionReducer]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/merge_decision_raw".format(base))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/merge_decision_refine_th".format(base))));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Avgbc - Refine Header Link - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[ExtractHeaderMergeDecisionMapper]);
    job.setReducerClass(classOf[ExtractHeaderMergeDecisionReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/merge_decision_refine_th".format(base))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/merge_decision".format(base))));
    job.waitForCompletion(true);
  }

  def updateClusters(prefix: String, base: String) = {
    updateClusterInfo(prefix, base);
    updateAdjCluster(prefix, base);
    updateClusterNode(prefix, base);

    fs.delete(new Path(Config.file("%s/adj_cluster".format(base))), true);
    fs.delete(new Path(Config.file("%s/cluster".format(base))), true);
    fs.delete(new Path(Config.file("%s/cluster_node".format(base))), true);

    fs.rename(new Path(Config.file("%s/adj_cluster_updated".format(base))),
      new Path(Config.file("%s/adj_cluster".format(base))))
    fs.rename(new Path(Config.file("%s/cluster_updated".format(base))),
      new Path(Config.file("%s/cluster".format(base))))
    fs.rename(new Path(Config.file("%s/cluster_node_updated".format(base))),
      new Path(Config.file("%s/cluster_node".format(base))))
  }

  def updateClusterInfo(prefix: String, base: String) = {
    fs.delete(new Path(Config.file("%s/cluster_info_nofrom".format(base))), true);

    var job = Job.getInstance(conf, "Avgbc - Update ClusterInfo - RemoveFrom -%s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[RemoveFromMapper]);
    job.setReducerClass(classOf[RemoveFromReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/cluster_info".format(base))));
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/merge_decision".format(base))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/cluster_info_nofrom".format(base))));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Avgbc - Update ClusterInfo - AddToSize -%s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AddToMapper]);
    job.setReducerClass(classOf[AddToReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/cluster_info_nofrom".format(base))));
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/merge_decision".format(base))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/cluster_info_updated".format(base))));
    job.waitForCompletion(true);
  }

  def updateAdjCluster(prefix: String, base: String) = {

    fs.delete(new Path(Config.file("%s/adj_cluster_updated_left".format(base))), true);
    fs.delete(new Path(Config.file("%s/adj_cluster_updated_dup".format(base))), true);

    var job = Job.getInstance(conf, "Avgbc - Left Update Adj Cluster - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[UpdateLeftAdjClusterMapper]);
    job.setReducerClass(classOf[UpdateLeftAdjClusterReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/adj_cluster".format(base))));
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/merge_decision".format(base))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/adj_cluster_updated_left".format(base))));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Avgbc - Right Update Adj Cluster - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[UpdateRightAdjClusterMapper]);
    job.setReducerClass(classOf[UpdateRightAdjClusterReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/adj_cluster_updated_left".format(base))));
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/merge_decision".format(base))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/adj_cluster_updated_dup".format(base))));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Avgbc - Adj Cluster Distinct Data - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[DistinctMapper]);
    job.setReducerClass(classOf[DistinctReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/adj_cluster_updated_dup".format(base))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/adj_cluster_updated".format(base))));
    job.waitForCompletion(false);
  }

  def updateClusterNode(prefix: String, base: String) = {
    var job = Job.getInstance(conf, "Avgbc - Update Cluster Node - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[UpdateClusterNodeMapper]);
    job.setReducerClass(classOf[UpdateClusterNodeReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/cluster_node".format(base))));
    FileInputFormat.addInputPath(job, new Path(Config.file("%s/merge_decision".format(base))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("%s/cluster_node_updated".format(base))));
    job.waitForCompletion(true);
  }
  def emptyMergeDecision(base: String): Boolean = {
    var contentSummary = fs.getContentSummary(new Path(Config.file("%s/merge_decision_raw".format(base))));
    return 0 == contentSummary.getLength;
  }
}