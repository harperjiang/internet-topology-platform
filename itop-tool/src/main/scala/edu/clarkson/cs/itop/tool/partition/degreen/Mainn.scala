package edu.clarkson.cs.itop.tool.partition.degreen

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

object Mainn extends App {

  var conf = new Configuration();

  FileSystem.get(conf).delete(new Path(Config.file("degreen")), true);

  // Prepare Data
  FileUtil.copy(FileSystem.get(conf), new Path(Config.file("common/adj_node")), FileSystem.get(conf), new Path(Config.file("degreen/adj_cluster")), false, true, conf);
  FileUtil.copy(FileSystem.get(conf), new Path(Config.file("common/node_degree")), FileSystem.get(conf), new Path(Config.file("degreen/cluster")), false, true, conf);

  var job = Job.getInstance(conf, "Degree n - Prepare Cluster Node Mapping");
  job.setJarByClass(Mainn.getClass);
  job.setMapperClass(classOf[PrepareClusterNodeMapper]);
  job.setNumReduceTasks(0);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(job, new Path(Config.file("common/node_degree")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/cluster_node")));
  job.waitForCompletion(true);

}

