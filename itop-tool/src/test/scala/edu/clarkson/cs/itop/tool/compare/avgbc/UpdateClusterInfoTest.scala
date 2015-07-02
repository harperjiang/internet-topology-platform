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
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

class UpdateClusterInfoTest {

  @Test
  def testUpdateClusterInfo1: Unit = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/update_cluster_info_1/output"), true);
    var job = Job.getInstance(conf, "Avgbc - Update ClusterInfo - RemoveFrom");
    job.setMapperClass(classOf[RemoveFromMapper]);
    job.setReducerClass(classOf[RemoveFromReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_cluster_info_1/cluster_info"));
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_cluster_info_1/merge_decision"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/update_cluster_info_1/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/compare/avgbc/update_cluster_info_1/result",
      "testdata/compare/avgbc/update_cluster_info_1/output/part-r-00000"))
  }

  @Test
  def testUpdateClusterInfo2: Unit = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/update_cluster_info_2/output"), true);
    var job = Job.getInstance(conf, "Avgbc - Update ClusterInfo - AddToSize");
    job.setMapperClass(classOf[AddToMapper]);
    job.setReducerClass(classOf[AddToReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_cluster_info_2/cluster_info_nofrom"));
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/update_cluster_info_2/merge_decision"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/update_cluster_info_2/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/compare/avgbc/update_cluster_info_2/result",
      "testdata/compare/avgbc/update_cluster_info_2/output/part-r-00000"))
  }
}