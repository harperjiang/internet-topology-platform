package edu.clarkson.cs.itop.tool.partition.exp

import org.junit.Test
import org.junit.Assert._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import edu.clarkson.cs.itop.tool.Config

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.IntWritable

import edu.clarkson.cs.itop.tool.FileCompare

import org.apache.hadoop.fs.FileSystem

class PrepareClusterNodeTest {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/exp/prepare_cluster_node/output"), true);

    var job = Job.getInstance(conf, "Degree n - Prepare Cluster Node Mapping");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[PrepareClusterNodeMapper]);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path("testdata/exp/prepare_cluster_node/input"))
    FileOutputFormat.setOutputPath(job, new Path("testdata/exp/prepare_cluster_node/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/exp/prepare_cluster_node/result", "testdata/exp/prepare_cluster_node/output/part-r-00000"))

  }
}