package edu.clarkson.cs.itop.tool.common

import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.types.IntArrayWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.FileSystem
import edu.clarkson.cs.itop.tool.FileCompare

class AdjNodeTest {

  @Test
  def test(): Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/adj_node/output"), true);

    var job = Job.getInstance(conf, "Adjacent Node");
    job.setJarByClass(Prepare.getClass);
    job.setMapperClass(classOf[AdjNodeMapper]);
    job.setReducerClass(classOf[AdjNodeReducer]);
    job.setMapOutputKeyClass(classOf[IntArrayWritable]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);

    FileInputFormat.addInputPath(job, new Path("testdata/adj_node/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/adj_node/output"));
    job.waitForCompletion(false);

    
    assertTrue(FileCompare.compare("testdata/adj_node/result","testdata/adj_node/output/part-r-00000"))
  }
}