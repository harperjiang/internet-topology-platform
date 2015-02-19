package edu.clarkson.cs.itop.tool.partition.degreen

import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

class ClusterLinkPartitionTest {

  @Test
  def testClusterDegreeMapper: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degreen/cluster_link_partition/cluster_node_degree"), true);
    fs.delete(new Path("testdata/degreen/cluster_link_partition/cluster_link"), true);
    fs.delete(new Path("testdata/degreen/cluster_link_partition/output"), true);

    var job = Job.getInstance(conf, "Test Cluster Degree");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[ClusterDegreeMapper]);
    job.setReducerClass(classOf[ClusterDegreeReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/cluster_link_partition/cluster"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/cluster_link_partition/cluster_node"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/cluster_link_partition/cluster_node_degree"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/cluster_link_partition/cluster_node_degree_result",
      "testdata/degreen/cluster_link_partition/cluster_node_degree/part-r-00000"));

    job = Job.getInstance(conf, "Test Cluster Link");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[ClusterLinkMapper]);
    job.setReducerClass(classOf[ClusterLinkReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/cluster_link_partition/node_link"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/cluster_link_partition/cluster_node_degree"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/cluster_link_partition/cluster_link"));
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compare("testdata/degreen/cluster_link_partition/cluster_link_result",
      "testdata/degreen/cluster_link_partition/cluster_link/part-r-00000"));
    
    job = Job.getInstance(conf, "Test Cluster Partition");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[LinkPartitionMapper]);
    job.setReducerClass(classOf[LinkPartitionReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/cluster_link_partition/cluster_link"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/cluster_link_partition/output"));
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compare("testdata/degreen/cluster_link_partition/result",
      "testdata/degreen/cluster_link_partition/output/part-r-00000"));
  }

}