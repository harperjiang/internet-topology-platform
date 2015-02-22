package edu.clarkson.cs.itop.tool.genpart

import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.partition.degree.Main1
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.io.Text

class NodePartitionJoinTest {
  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/genpart/node_partition_join/join_output"), true);

    var job = Job.getInstance(conf, "Node Partition Join");
    job.setJarByClass(Main1.getClass);
    job.setMapperClass(classOf[NodePartitionJoinMapper]);
    job.setReducerClass(classOf[NodePartitionJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/genpart/node_partition_join/link_partition"));
    FileInputFormat.addInputPath(job, new Path("testdata/genpart/node_partition_join/node_link"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/genpart/node_partition_join/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/genpart/node_partition_join/result", "testdata/genpart/node_partition_join/output/part-r-00000"))
  }

  @Test
  def testInfo: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/genpart/node_partition_info_join/output"), true);

    var job = Job.getInstance(conf, "Node Partition Info Join");
    job.setJarByClass(Main1.getClass);
    job.setMapperClass(classOf[NodePartitionInfoJoinMapper]);
    job.setReducerClass(classOf[NodePartitionInfoJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/genpart/node_partition_info_join/kapar-midar-iff.nodes"));
    FileInputFormat.addInputPath(job, new Path("testdata/genpart/node_partition_info_join/node_partition"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/genpart/node_partition_info_join/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/genpart/node_partition_info_join/result", "testdata/genpart/node_partition_info_join/output/part-r-00000"))
  }
}