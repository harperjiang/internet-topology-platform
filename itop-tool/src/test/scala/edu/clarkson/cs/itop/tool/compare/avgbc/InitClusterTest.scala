package edu.clarkson.cs.itop.tool.compare.avgbc

import org.junit.Test
import org.junit.Assert._
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.FileSystem

class InitClusterTest {

  @Test
  def testInitClusterInfo(): Unit = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/init_cluster_info/output"), true);
    var job = Job.getInstance(conf, "Avgbc - Init Cluster Info");
    job.setMapperClass(classOf[InitClusterInfoMapper]);
    job.setReducerClass(classOf[InitClusterInfoReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/init_cluster_info/node_partition_raw"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/init_cluster_info/output"))
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/compare/avgbc/init_cluster_info/result",
      "testdata/compare/avgbc/init_cluster_info/output/part-r-00000"))
  }

  @Test
  def testInitClusterNode(): Unit = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/init_cluster_node/output"), true);
    var job = Job.getInstance(conf, "Avgbc - Init Cluster Node");
    job.setMapperClass(classOf[InitClusterNodeMapper]);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/init_cluster_node/input"))
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/init_cluster_node/output"))
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/compare/avgbc/init_cluster_node/result",
      "testdata/compare/avgbc/init_cluster_node/output/part-m-00000"))
  }
}