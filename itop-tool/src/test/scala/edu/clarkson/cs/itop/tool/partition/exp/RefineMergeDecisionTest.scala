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
import org.apache.hadoop.io.Text

import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

class RefineMergeDecisionTest {

  @Test
  def testRemoveTwoHead: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/exp/refine_two_header/output"), true);

    var job = Job.getInstance(conf, "Test Refine Two Head");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[RemoveTwoHeadMergeDecisionMapper]);
    job.setReducerClass(classOf[RemoveTwoHeadMergeDecisionReducer]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/exp/refine_two_header/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/exp/refine_two_header/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/exp/refine_two_header/result",
      "testdata/exp/refine_two_header/output/part-r-00000"))
  }

  @Test
  def testExtractHeader: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/exp/refine_header_link/output"), true);

    var job = Job.getInstance(conf, "Test Refine Header Link");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[ExtractHeaderMergeDecisionMapper]);
    job.setReducerClass(classOf[ExtractHeaderMergeDecisionReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);

    FileInputFormat.addInputPath(job, new Path("testdata/exp/refine_header_link/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/exp/refine_header_link/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/exp/refine_header_link/result",
      "testdata/exp/refine_header_link/output/part-r-00000"))
  }
}