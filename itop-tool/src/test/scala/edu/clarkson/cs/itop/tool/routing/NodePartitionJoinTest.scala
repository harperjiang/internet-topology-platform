package edu.clarkson.cs.itop.tool.routing

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

class NodePartitionJoinTest {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/routing_node_partition_join/output"), true);

    var job = Job.getInstance(conf, "Routing Node Partition Join");
    job.setMapperClass(classOf[NodePartitionJoinMapper]);
    job.setReducerClass(classOf[NodePartitionJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/routing_node_partition_join/node_link"))
    FileInputFormat.addInputPath(job, new Path("testdata/routing_node_partition_join/link_partition"))
    FileOutputFormat.setOutputPath(job, new Path("testdata/routing_node_partition_join/output"))
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compareContent("testdata/routing_node_partition_join/result", "testdata/routing_node_partition_join/output/part-r-00000"))
  }
}