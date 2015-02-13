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
import org.apache.hadoop.io.IntWritable

class NodePartitionFilterTest {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/routing_node_partition_filter/output"), true);

    var job = Job.getInstance(conf, "Routing Node Partition Filter")
    job.setMapperClass(classOf[NodePartitionFilterMapper])
    job.setReducerClass(classOf[NodePartitionFilterReducer])
    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path("testdata/routing_node_partition_filter/input"))
    FileOutputFormat.setOutputPath(job, new Path("testdata/routing_node_partition_filter/output"))
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compareContent("testdata/routing_node_partition_filter/result", "testdata/routing_node_partition_filter/output/part-r-00000"))
  }
}