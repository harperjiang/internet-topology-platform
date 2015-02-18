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
import org.apache.hadoop.io.Text
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

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
    fs.delete(new Path("testdata/degreen/update_cluster/output"), true);

    var job = Job.getInstance(conf, "Test Update Cluster");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[UpdateAdjClusterMapper]);
    job.setReducerClass(classOf[UpdateAdjClusterReducer]);
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
  def testUpdateClusterNode: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degreen/update_cluster_node/output"), true);

    var job = Job.getInstance(conf, "Test Update Cluster");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[UpdateClusterNodeMapper]);
    job.setReducerClass(classOf[UpdateClusterNodeReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_cluster_node/cluster"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/update_cluster_node/merge_decision"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/update_cluster_node/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/update_cluster_node/result",
      "testdata/degreen/update_cluster_node/output/part-r-00000"))
  }
}