package edu.clarkson.cs.itop.tool.common

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
import org.apache.hadoop.io.NullWritable

class MergeTest {
  @Test
  def test(): Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/merge/output"), true);

    var job = Job.getInstance(conf, "Merge Data");
    job.setJarByClass(Prepare.getClass);
    job.setNumReduceTasks(0)
    job.setMapperClass(classOf[MergeMapper]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[NullWritable]);
    for (i <- 0 to 9) {
      FileInputFormat.addInputPath(job, new Path("testdata/merge/input_%d".format(i)));
    }
    FileOutputFormat.setOutputPath(job, new Path("testdata/merge/output"));
    job.waitForCompletion(false);
    
    assertTrue(FileCompare.compareContent("testdata/merge/result", "testdata/merge/output"))
  }
}