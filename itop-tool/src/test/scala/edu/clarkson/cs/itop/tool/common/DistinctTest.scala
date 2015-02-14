package edu.clarkson.cs.itop.tool.common

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
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

class DistinctTest {
  @Test
  def test(): Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/distinct/output"), true);

    var job = Job.getInstance(conf, "Distinct Data");
    job.setJarByClass(classOf[DistinctTest]);
    job.setMapperClass(classOf[DistinctMapper]);
    job.setReducerClass(classOf[DistinctReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);

    FileInputFormat.addInputPath(job, new Path("testdata/distinct/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/distinct/output"));
    job.waitForCompletion(false);

    assertTrue(FileCompare.compare("testdata/distinct/result", "testdata/distinct/output/part-r-00000"))
  }
}