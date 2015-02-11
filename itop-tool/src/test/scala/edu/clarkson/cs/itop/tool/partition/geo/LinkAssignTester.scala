package edu.clarkson.cs.itop.tool.partition.geo

import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class LinkAssignTester {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/link_assign/output"), true);

    var job = Job.getInstance(conf, "Link Assign Geo");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[LinkAssignMapper]);
    job.setReducerClass(classOf[LinkAssignReducer]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);

    FileInputFormat.addInputPath(job, new Path("testdata/link_assign/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/link_assign/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compareContent("testdata/link_assign/result", "testdata/link_assign/output/part-r-00000"))
  }
}