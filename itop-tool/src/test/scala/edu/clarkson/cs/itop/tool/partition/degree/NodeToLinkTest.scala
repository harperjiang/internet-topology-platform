package edu.clarkson.cs.itop.tool.partition.degree

import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.common.Prepare
import edu.clarkson.cs.itop.tool.common.NodeDegreeReducer
import edu.clarkson.cs.itop.tool.common.NodeDegreeMapper
import org.apache.hadoop.io.IntWritable

class NodeToLinkTest {
  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/node_link/output"), true);

    var job = Job.getInstance(conf, "Node to Link");
    job.setJarByClass(Prepare.getClass);
    job.setMapperClass(classOf[NodeToLinkMapper]);
    job.setNumReduceTasks(0)
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntWritable]);

    FileInputFormat.addInputPath(job, new Path("testdata/node_link/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/node_link/output"));
    job.waitForCompletion(false);

    assertTrue(FileCompare.compareContent("testdata/node_link/result", "testdata/node_link/output/part-m-00000"))
  }
}