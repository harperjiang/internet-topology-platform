package edu.clarkson.cs.itop.tool.partition.degree

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
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

class NodeExpandTest {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/node_expand/output"), true);

    var epjob = Job.getInstance(conf, "Degree n - Expand Node");
    epjob.setJarByClass(Mainn.getClass);
    epjob.setMapperClass(classOf[NodeExpandMapper]);
    epjob.setReducerClass(classOf[NodeExpandReducer]);
    epjob.setMapOutputKeyClass(classOf[StringArrayWritable]);
    epjob.setMapOutputValueClass(classOf[StringArrayWritable]);
    epjob.setOutputKeyClass(classOf[IntWritable]);
    epjob.setOutputValueClass(classOf[IntWritable]);
    epjob.setPartitionerClass(classOf[KeyPartitioner]);
    epjob.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(epjob, new Path("testdata/node_expand/node_expand_1"));
    FileInputFormat.addInputPath(epjob, new Path("testdata/node_expand/adj_node"));
    FileOutputFormat.setOutputPath(epjob, new Path("testdata/node_expand/output"));
    epjob.waitForCompletion(true);

    assertTrue(FileCompare.compareContent("testdata/node_expand/result", "testdata/node_expand/output/part-r-00000"))

  }
}