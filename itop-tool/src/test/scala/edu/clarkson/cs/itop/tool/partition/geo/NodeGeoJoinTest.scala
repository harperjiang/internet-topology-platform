package edu.clarkson.cs.itop.tool.partition.geo

import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

class NodeGeoJoinTest {

  @Test
  def test: Unit = {

    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/node_geo_join/output"), true);

    var job = Job.getInstance(conf, "Node Geo Join");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[NodeGeoJoinMapper]);
    job.setReducerClass(classOf[NodeGeoJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    
    FileInputFormat.addInputPath(job, new Path("testdata/node_geo_join/geo_partition"));
    FileInputFormat.addInputPath(job, new Path("testdata/node_geo_join/node_geo"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/node_geo_join/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compareContent("testdata/node_geo_join/result", "testdata/node_geo_join/output/part-r-00000"))

  }
}