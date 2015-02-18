package edu.clarkson.cs.itop.tool.common

import org.junit.Test
import org.junit.Assert._
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.partition.geo.Main
import org.apache.hadoop.mapreduce.Job
import edu.clarkson.cs.itop.tool.FileCompare

class CounterTest {

  @Test
  def test: Unit = {

    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/common/counter/output"), true);

    conf.set(CounterParam.KEY_INDEX, "1");
    var job = Job.getInstance(conf, "Geo Count");
    job.setMapperClass(classOf[CounterMapper]);
    job.setReducerClass(classOf[CounterReducer]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path("testdata/common/counter/input"))
    FileOutputFormat.setOutputPath(job, new Path("testdata/common/counter/output"))
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/common/counter/result", "testdata/common/counter/output/part-r-00000"))
  }
}