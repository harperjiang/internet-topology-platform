package edu.clarkson.cs.itop.tool.prepare

import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

class CountNodeTest {
  @Test
  def test(): Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/count_node/output"), true);

    var job = Job.getInstance(conf, "Count Node");
    job.setJarByClass(Prepare.getClass);
    job.setMapperClass(classOf[NodeDegreeMapper]);
    job.setReducerClass(classOf[NodeDegreeReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);

    FileInputFormat.addInputPath(job, new Path("testdata/count_node/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/count_node/output"));
    job.waitForCompletion(false);

    assertTrue(FileCompare.compare("testdata/count_node/result", "testdata/count_node/output/part-r-00000"))
  }
}