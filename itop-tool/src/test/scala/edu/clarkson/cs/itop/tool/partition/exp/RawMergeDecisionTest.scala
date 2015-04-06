package edu.clarkson.cs.itop.tool.partition.exp

import org.junit.Test
import org.junit.Assert._

import edu.clarkson.cs.itop.tool.FileCompare

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.partition.degree.Main1
import edu.clarkson.cs.itop.tool.partition.exp.Mainn;
import edu.clarkson.cs.itop.tool.types.KeyPartitioner

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

class RawMergeDecisionTest {

  @Test
  def testMergeDecisionLeftDegree: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degreen/md_leftdegree/output"), true);

    var job = Job.getInstance(conf, "Test Merge Decision Left Degree");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[MergeDecisionLeftDegreeMapper]);
    job.setReducerClass(classOf[MergeDecisionLeftDegreeReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/md_leftdegree/cluster"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/md_leftdegree/adj_cluster"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/md_leftdegree/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/md_leftdegree/result",
      "testdata/degreen/md_leftdegree/output/part-r-00000"))
  }

  @Test
  def testMergeDecisionRightDegree: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degreen/md_rightdegree/output"), true);

    var job = Job.getInstance(conf, "Test Merge Decision Right Degree");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[MergeDecisionRightDegreeMapper]);
    job.setReducerClass(classOf[MergeDecisionRightDegreeReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/md_rightdegree/cluster"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/md_rightdegree/adj_cluster_left"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/md_rightdegree/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/md_rightdegree/result",
      "testdata/degreen/md_rightdegree/output/part-r-00000"))

  }

  @Test
  def testMergeDecision: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degreen/merge_decision/output"), true);

    var job = Job.getInstance(conf, "Test Merge Decision");
    job.setJarByClass(Mainn.getClass);
    job.setMapperClass(classOf[MergeDecisionMapper]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/merge_decision/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/merge_decision/output"));
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compare("testdata/degreen/merge_decision/result",
      "testdata/degreen/merge_decision/output/part-r-00000"))
  }
}