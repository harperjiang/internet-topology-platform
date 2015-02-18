package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Assert.assertTrue
import org.junit.Test

import edu.clarkson.cs.itop.tool.FileCompare

class NodeGeoTest {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/geo/node_geo/output"), true);

    var job = Job.getInstance(conf, "Node Geo");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[NodeGeoMapper]);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);

    FileInputFormat.addInputPath(job, new Path("testdata/geo/node_geo/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/geo/node_geo/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compareContent("testdata/geo/node_geo/result", "testdata/geo/node_geo/output/part-m-00000"))
  }
}