package edu.clarkson.cs.itop.tool.partition.degree

import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.FileSystem

class JoinLinkDegreeTester {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degree/join_link/output"), true);

    var icjob1 = Job.getInstance(conf, "Join Link Degree");
    icjob1.setJarByClass(Main1.getClass);
    icjob1.setMapperClass(classOf[JoinLinkDegreeMapper]);
    icjob1.setReducerClass(classOf[JoinLinkDegreeReducer]);
    icjob1.setMapOutputKeyClass(classOf[StringArrayWritable]);
    icjob1.setMapOutputValueClass(classOf[StringArrayWritable]);
    icjob1.setOutputKeyClass(classOf[IntWritable]);
    icjob1.setOutputValueClass(classOf[Text]);
    icjob1.setPartitionerClass(classOf[KeyPartitioner]);
    icjob1.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(icjob1, new Path("testdata/degree/join_link/node_link"));
    FileInputFormat.addInputPath(icjob1, new Path("testdata/degree/join_link/node_degree"));
    FileOutputFormat.setOutputPath(icjob1, new Path("testdata/degree/join_link/output"));
    icjob1.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degree/join_link/result", "testdata/degree/join_link/output/part-r-00000"))

  }
}