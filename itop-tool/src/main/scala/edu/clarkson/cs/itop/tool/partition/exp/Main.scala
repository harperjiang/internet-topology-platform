package edu.clarkson.cs.itop.tool.partition.exp

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
import edu.clarkson.cs.itop.tool.common.DistinctMapper
import edu.clarkson.cs.itop.tool.common.DistinctReducer
import edu.clarkson.cs.itop.tool.common.MergeMapper
import edu.clarkson.cs.itop.tool.partition.degree.JoinLinkDegreeMapper
import edu.clarkson.cs.itop.tool.partition.degree.JoinLinkDegreeReducer
import edu.clarkson.cs.itop.tool.partition.degree.Main1
import edu.clarkson.cs.itop.tool.partition.degree.MaxDegreeMapper
import edu.clarkson.cs.itop.tool.partition.degree.MaxDegreeReducer
import edu.clarkson.cs.itop.tool.partition.degree.NodeExpandMapper
import edu.clarkson.cs.itop.tool.partition.degree.NodeExpandReducer
import edu.clarkson.cs.itop.tool.partition.degree.PartitionLinkMapper
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text

object Main extends App {

  var conf = new Configuration();

  FileSystem.get(conf).delete(new Path(Config.file("exp")), true);

  prepareData(conf)

  // Process Clusters
  for (i <- 0 to Param.exp_n - 1) {
    rawMergeDecision(conf)
    refineMergeDecision(conf, i)
    updateClusters(conf, i)
  }
  generateLinkPartition(conf)

  def prepareData(conf: Configuration): Unit = {
    // Prepare Data
    FileUtil.copy(FileSystem.get(conf), new Path(Config.file("common/adj_node")), FileSystem.get(conf),
      new Path(Config.file("exp/adj_cluster")), false, true, conf);
    FileUtil.copy(FileSystem.get(conf), new Path(Config.file("common/node_degree")), FileSystem.get(conf),
      new Path(Config.file("exp/cluster")), false, true, conf);

    var job = Job.getInstance(conf, "Fold n - Prepare Cluster Node Mapping");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[PrepareClusterNodeMapper]);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(Config.file("common/node_degree")))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/cluster_node")));
    job.waitForCompletion(true);
  }

  def rawMergeDecision(conf: Configuration): Unit = {
    FileSystem.get(conf).delete(new Path(Config.file("exp/adj_cluster_left")), true);
    FileSystem.get(conf).delete(new Path(Config.file("exp/merge_decision_dup")), true);
    FileSystem.get(conf).delete(new Path(Config.file("exp/merge_decision_raw")), true);

    var job = Job.getInstance(conf, "Raw Merge Decision Left Degree");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[MergeDecisionLeftDegreeMapper]);
    job.setReducerClass(classOf[MergeDecisionLeftDegreeReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/cluster")));
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/adj_cluster")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/adj_cluster_left")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Raw Merge Decision Right Degree");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[MergeDecisionRightDegreeMapper]);
    job.setReducerClass(classOf[MergeDecisionRightDegreeReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/cluster")));
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/adj_cluster_left")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/merge_decision_dup")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Raw Merge Decision");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[MergeDecisionMapper]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/merge_decision_dup")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/merge_decision_raw")));
    job.waitForCompletion(true);
  }

  def refineMergeDecision(conf: Configuration, round: Int): Unit = {
    FileSystem.get(conf).delete(new Path(Config.file("exp/merge_decision_refine_th")), true);
    if (round > 0) {
      FileSystem.get(conf).rename(new Path(Config.file("exp/merge_decision")),
        new Path(Config.file("exp/merge_decision_%d".format(round))))
    } else {
      FileSystem.get(conf).delete(new Path(Config.file("exp/merge_decision")), true);
    }

    var job = Job.getInstance(conf, "Refine Two Head");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[RemoveTwoHeadMergeDecisionMapper]);
    job.setReducerClass(classOf[RemoveTwoHeadMergeDecisionReducer]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/merge_decision_raw")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/merge_decision_refine_th")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Refine Header Link");
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
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/merge_decision_refine_th")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/merge_decision")));
    job.waitForCompletion(true);
  }

  def updateClusters(conf: Configuration, round: Int): Unit = {
    FileSystem.get(conf).delete(new Path(Config.file("exp/adj_cluster_updated_left")), true);
    FileSystem.get(conf).delete(new Path(Config.file("exp/adj_cluster_updated_dup")), true);

    var job = Job.getInstance(conf, "Update Cluster");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[UpdateClusterMapper]);
    job.setReducerClass(classOf[UpdateClusterReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/cluster")));
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/merge_decision")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/cluster_updated")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Update Cluster Node");
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
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/cluster_node")));
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/merge_decision")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/cluster_node_updated")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Left Update Adj Cluster");
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
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/adj_cluster")));
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/merge_decision")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/adj_cluster_updated_left")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Right Update Adj Cluster");
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
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/adj_cluster_updated_left")));
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/merge_decision")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/adj_cluster_updated_dup")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Adj Cluster Distinct Data");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[DistinctMapper]);
    job.setReducerClass(classOf[DistinctReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/adj_cluster_updated_dup")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/adj_cluster_updated")));
    job.waitForCompletion(false);

    FileSystem.get(conf).rename(new Path(Config.file("exp/adj_cluster")), new Path(Config.file("exp/adj_cluster_%d".format(round))))
    FileSystem.get(conf).rename(new Path(Config.file("exp/cluster")), new Path(Config.file("exp/cluster_%d".format(round))))
    FileSystem.get(conf).rename(new Path(Config.file("exp/cluster_node")), new Path(Config.file("exp/cluster_node_%d".format(round))))

    FileSystem.get(conf).rename(new Path(Config.file("exp/adj_cluster_updated")), new Path(Config.file("exp/adj_cluster")))
    FileSystem.get(conf).rename(new Path(Config.file("exp/cluster_updated")), new Path(Config.file("exp/cluster")))
    FileSystem.get(conf).rename(new Path(Config.file("exp/cluster_node_updated")), new Path(Config.file("exp/cluster_node")))
  }

  def generateLinkPartition(conf: Configuration): Unit = {
    var job = Job.getInstance(conf, "Cluster Degree");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[ClusterDegreeMapper]);
    job.setReducerClass(classOf[ClusterDegreeReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/cluster")));
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/cluster_node")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/cluster_node_degree")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Cluster Link");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[ClusterLinkMapper]);
    job.setReducerClass(classOf[ClusterLinkReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/common/node_link")));
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/cluster_node_degree")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/cluster_link")));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Cluster Partition");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[LinkPartitionMapper]);
    job.setReducerClass(classOf[LinkPartitionReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("exp/cluster_link")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("exp/link_partition")));
    job.waitForCompletion(true);
  }
}

