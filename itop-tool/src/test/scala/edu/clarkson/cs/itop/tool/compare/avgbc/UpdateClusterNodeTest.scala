package edu.clarkson.cs.itop.tool.compare.avgbc

import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.Text

class UpdateClusterNodeTest {

  @Test
  def testUpdateClusterNode:Unit = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/update_cluster_node/output"), true);
    
    var job = Job.getInstance(conf, "Avgbc - Update Cluster Node");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[UpdateClusterNodeMapper]);
    job.setReducerClass(classOf[UpdateClusterNodeReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_cluster_node/cluster_node"));
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_cluster_node/merge_decision"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/update_cluster_node/output"));
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compare("testdata/compare/avgbc/update_cluster_node/result",
      "testdata/compare/avgbc/update_cluster_node/output/part-r-00000"))
  }
}