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

class NodeMachineTest {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/routing_node_machine/output"), true);

    var job = Job.getInstance(conf, "Routing Node Machine");
    job.setMapperClass(classOf[NodeMachineMapper]);
    job.setReducerClass(classOf[NodeMachineReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/routing_node_machine/node_link"))
    FileInputFormat.addInputPath(job, new Path("testdata/routing_node_machine/link_partition"))
    FileOutputFormat.setOutputPath(job, new Path("testdata/routing_node_machine/output"))
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compareContent("testdata/routing_node_machine/result", "testdata/routing_node_machine/output/part-r-00000"))
  }
}