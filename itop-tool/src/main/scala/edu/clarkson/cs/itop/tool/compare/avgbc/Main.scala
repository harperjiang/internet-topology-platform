package edu.clarkson.cs.itop.tool.compare.avgbc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.io.Text
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.fs.FileUtil
import edu.clarkson.cs.itop.tool.partition.exp.UpdateLeftAdjClusterMapper
import edu.clarkson.cs.itop.tool.partition.exp.UpdateRightAdjClusterMapper
import edu.clarkson.cs.itop.tool.partition.exp.UpdateRightAdjClusterReducer
import edu.clarkson.cs.itop.tool.partition.exp.ClusterLinkMapper
import edu.clarkson.cs.itop.tool.partition.exp.ClusterLinkReducer
import edu.clarkson.cs.itop.tool.common.DistinctReducer
import edu.clarkson.cs.itop.tool.partition.exp.UpdateLeftAdjClusterReducer
import edu.clarkson.cs.itop.tool.partition.exp.ClusterDegreeMapper
import edu.clarkson.cs.itop.tool.partition.exp.RemoveTwoHeadMergeDecisionReducer
import edu.clarkson.cs.itop.tool.partition.exp.ExtractHeaderMergeDecisionReducer
import edu.clarkson.cs.itop.tool.partition.exp.UpdateClusterMapper
import edu.clarkson.cs.itop.tool.partition.exp.LinkPartitionReducer
import edu.clarkson.cs.itop.tool.partition.exp.UpdateClusterNodeReducer
import edu.clarkson.cs.itop.tool.partition.exp.ExtractHeaderMergeDecisionMapper
import edu.clarkson.cs.itop.tool.partition.exp.LinkPartitionMapper
import edu.clarkson.cs.itop.tool.partition.exp.ClusterDegreeReducer
import edu.clarkson.cs.itop.tool.partition.exp.UpdateClusterReducer
import edu.clarkson.cs.itop.tool.partition.exp.UpdateClusterNodeMapper
import edu.clarkson.cs.itop.tool.common.DistinctMapper
import edu.clarkson.cs.itop.tool.partition.exp.RemoveTwoHeadMergeDecisionMapper

object Main extends App {

  var conf = new Configuration();
  var fs = FileSystem.get(conf);

  def run(prefix: String) = {
    fs.delete(new Path(Config.file("compare/avgbc/%s".format(prefix))), true);
  }

  def initCluster(prefix: String) = {
    var job = Job.getInstance(conf, "Avgbc - Init Cluster - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[InitClusterNodeMapper]);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(Config.file("common/node_degree")))
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/cluster_node".format(prefix))))
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
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/cluster_info".format(prefix))))
    job.waitForCompletion(true);

    FileUtil.copy(FileSystem.get(conf), new Path(Config.file("common/adj_node")), FileSystem.get(conf),
      new Path(Config.file("compare/avgbc/%s/adj_cluster")), false, true, conf);
  }

  def rawMergeDecision(prefix: String) = {
    fs.delete(new Path(Config.file("compare/avgbc/%s/adj_cluster_left".format(prefix))));
    fs.delete(new Path(Config.file("compare/avgbc/%s/merge_decision_raw".format(prefix))));

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
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/cluster_info".format(prefix))));
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/adj_cluster".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/adj_cluster_left".format(prefix))))
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Raw Merge Right - %s".format(prefix));
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AdjClusterLeftJoinMapper]);
    job.setReducerClass(classOf[AdjClusterLeftJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/adj_cluster_left".format(prefix))));
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/cluster_info".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/merge_decision_raw".format(prefix))))
    job.waitForCompletion(true);
  }

  def refineMergeDecision(prefix: String) = {
    fs.delete(new Path(Config.file("compare/avgbc/%s/merge_decision_refine_th".format(prefix))), true);
    fs.delete(new Path(Config.file("compare/avgbc/%s/merge_decision".format(prefix))), true);

    var job = Job.getInstance(conf, "Refine Two Head");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[RemoveTwoHeadMergeDecisionMapper]);
    job.setReducerClass(classOf[RemoveTwoHeadMergeDecisionReducer]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setNumReduceTasks(6);
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/merge_decision_raw".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/merge_decision_refine_th".format(prefix))));
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
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/merge_decision_refine_th".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/merge_decision".format(prefix))));
    job.waitForCompletion(true);
  }

  def updateClusters(prefix: String): Unit = {
    FileSystem.get(conf).delete(new Path(Config.file("compare/avgbc/%s/adj_cluster_updated_left".format(prefix))), true);
    FileSystem.get(conf).delete(new Path(Config.file("compare/avgbc/%s/adj_cluster_updated_dup".format(prefix))), true);

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
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/cluster".format(prefix))));
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/merge_decision".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/cluster_updated".format(prefix))));
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
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/cluster_node".format(prefix))));
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/merge_decision".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/cluster_node_updated".format(prefix))));
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
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/adj_cluster".format(prefix))));
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/merge_decision".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/adj_cluster_updated_left".format(prefix))));
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
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/adj_cluster_updated_left".format(prefix))));
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/merge_decision".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/adj_cluster_updated_dup".format(prefix))));
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
    FileInputFormat.addInputPath(job, new Path(Config.file("compare/avgbc/%s/adj_cluster_updated_dup".format(prefix))));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("compare/avgbc/%s/adj_cluster_updated".format(prefix))));
    job.waitForCompletion(false);

    fs.delete(new Path(Config.file("compare/avgbc/%s/adj_cluster".format(prefix))), true);
    fs.delete(new Path(Config.file("compare/avgbc/%s/cluster".format(prefix))), true);
    fs.delete(new Path(Config.file("compare/avgbc/%s/cluster_node".format(prefix))), true);

    fs.rename(new Path(Config.file("compare/avgbc/%s/adj_cluster_updated".format(prefix))),
      new Path(Config.file("compare/avgbc/%s/adj_cluster".format(prefix))))
    fs.rename(new Path(Config.file("compare/avgbc/%s/cluster_updated".format(prefix))),
      new Path(Config.file("compare/avgbc/%s/cluster".format(prefix))))
    fs.rename(new Path(Config.file("compare/avgbc/%s/cluster_node_updated".format(prefix))),
      new Path(Config.file("compare/avgbc/%s/cluster_node".format(prefix))))
  }

}