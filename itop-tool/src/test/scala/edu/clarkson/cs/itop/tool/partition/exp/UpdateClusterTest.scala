package edu.clarkson.cs.itop.tool.partition.exp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Assert.assertTrue
import org.junit.Test

import edu.clarkson.cs.itop.tool.FileCompare
import edu.clarkson.cs.itop.tool.partition.exp.Mainn;
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.common.DistinctReducer
import edu.clarkson.cs.itop.tool.common.DistinctMapper

class UpdateClusterTest {

  @Test
  def testUpdateCluster: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degreen/update_cluster/output"), true);

    var job = Job.getInstance(conf, "Test Update Cluster");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[UpdateClusterMapper]);
    job.setReducerClass(classOf[UpdateClusterReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_cluster/cluster"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_cluster/merge_decision"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/update_cluster/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/update_cluster/result",
      "testdata/degreen/update_cluster/output/part-r-00000"))
  }

  @Test
  def testUpdateAdjCluster: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degreen/update_adj_cluster/output"), true);
    fs.delete(new Path("testdata/degreen/update_adj_cluster/adj_cluster_updated_left"), true);
    fs.delete(new Path("testdata/degreen/update_adj_cluster/adj_cluster_updated_dup"), true);

    var job = Job.getInstance(conf, "Test Left Update Adj Cluster");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[UpdateLeftAdjClusterMapper]);
    job.setReducerClass(classOf[UpdateLeftAdjClusterReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_adj_cluster/adj_cluster"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_adj_cluster/merge_decision"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/update_adj_cluster/adj_cluster_updated_left"));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Test Right Update Adj Cluster");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[UpdateRightAdjClusterMapper]);
    job.setReducerClass(classOf[UpdateRightAdjClusterReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_adj_cluster/adj_cluster_updated_left"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_adj_cluster/merge_decision"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/update_adj_cluster/adj_cluster_updated_dup"));
    job.waitForCompletion(true);

    job = Job.getInstance(conf, "Distinct Data");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[DistinctMapper]);
    job.setReducerClass(classOf[DistinctReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);

    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_adj_cluster/adj_cluster_updated_dup"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/update_adj_cluster/output"));
    job.waitForCompletion(false);

    assertTrue(FileCompare.compare("testdata/degreen/update_adj_cluster/result",
      "testdata/degreen/update_adj_cluster/output/part-r-00000"))
      
  }

  @Test
  def testUpdateClusterNode: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degreen/update_cluster_node/output"), true);

    var job = Job.getInstance(conf, "Test Update Cluster Node");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[UpdateClusterNodeMapper]);
    job.setReducerClass(classOf[UpdateClusterNodeReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_cluster_node/cluster_node"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_cluster_node/merge_decision"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/update_cluster_node/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/update_cluster_node/result",
      "testdata/degreen/update_cluster_node/output/part-r-00000"))
  }
}