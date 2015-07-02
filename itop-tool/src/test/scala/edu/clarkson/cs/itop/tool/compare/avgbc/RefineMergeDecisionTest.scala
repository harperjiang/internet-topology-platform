package edu.clarkson.cs.itop.tool.compare.avgbc

import org.junit.Assert._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.Text
import org.junit.Test
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.FileCompare

class RefineMergeDecisionTest {

  @Test
  def testRemoveTwoHead() = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/refine_merge_1/output"), true);
    var job = Job.getInstance(conf, "Avgbc - Refine Two Head");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[RemoveTwoHeadMergeDecisionMapper]);
    job.setReducerClass(classOf[RemoveTwoHeadMergeDecisionReducer]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/refine_merge_1/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/refine_merge_1/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/compare/avgbc/refine_merge_1/result",
      "testdata/compare/avgbc/refine_merge_1/output/part-r-00000"))
  }

  @Test
  def testHeadOfChain() = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/refine_merge_2/output"), true);
    var job = Job.getInstance(conf, "Avgbc - Refine Header Link ");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[ExtractHeaderMergeDecisionMapper]);
    job.setReducerClass(classOf[ExtractHeaderMergeDecisionReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/refine_merge_2/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/refine_merge_2/output"));
    job.waitForCompletion(true);
    
    assertTrue(FileCompare.compare("testdata/compare/avgbc/refine_merge_2/result",
      "testdata/compare/avgbc/refine_merge_2/output/part-r-00000"))
  }
}