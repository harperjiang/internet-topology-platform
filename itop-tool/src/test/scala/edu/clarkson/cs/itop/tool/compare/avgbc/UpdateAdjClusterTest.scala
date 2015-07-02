package edu.clarkson.cs.itop.tool.compare.avgbc

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
import edu.clarkson.cs.itop.tool.common.DistinctMapper
import edu.clarkson.cs.itop.tool.common.DistinctReducer
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

class UpdateAdjClusterTest {

  @Test
  def testUpdateAdjCluster1:Unit = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/update_adj_cluster_1/output"), true);
    var job = Job.getInstance(conf, "Avgbc - Left Update Adj Cluster");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[UpdateLeftAdjClusterMapper]);
    job.setReducerClass(classOf[UpdateLeftAdjClusterReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_adj_cluster_1/adj_cluster"));
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_adj_cluster_1/merge_decision"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/update_adj_cluster_1/output"));
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compare("testdata/compare/avgbc/update_adj_cluster_1/result",
      "testdata/compare/avgbc/update_adj_cluster_1/output/part-r-00000"))
  }
  
  @Test
  def testUpdateAdjCluster2:Unit = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/update_adj_cluster_2/output"), true);
    var job = Job.getInstance(conf, "Avgbc - Right Update Adj Cluster");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[UpdateRightAdjClusterMapper]);
    job.setReducerClass(classOf[UpdateRightAdjClusterReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_adj_cluster_2/adj_cluster_updated_left"));
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_adj_cluster_2/merge_decision"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/update_adj_cluster_2/output"));
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compare("testdata/compare/avgbc/update_adj_cluster_2/result",
      "testdata/compare/avgbc/update_adj_cluster_2/output/part-r-00000"))
  }
  
  @Test
  def testUpdateAdjCluster3:Unit = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/update_adj_cluster_3/output"), true);
    var job = Job.getInstance(conf, "Avgbc - Adj Cluster Distinct Data");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[DistinctMapper]);
    job.setReducerClass(classOf[DistinctReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_adj_cluster_3/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/update_adj_cluster_3/output"));
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compare("testdata/compare/avgbc/update_adj_cluster_3/result",
      "testdata/compare/avgbc/update_adj_cluster_3/output/part-r-00000"))
  }
}