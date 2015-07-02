package edu.clarkson.cs.itop.tool.compare.avgbc

import org.junit.Assert._
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.FileCompare
import org.junit.Test

class MergeDecisionTest {

  @Test
  def testMergeStep1: Unit = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/merge_decision_1/output"), true);

    var job = Job.getInstance(conf, "Raw Merge Left");
    job.setMapperClass(classOf[AdjClusterLeftJoinMapper]);
    job.setReducerClass(classOf[AdjClusterLeftJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/merge_decision_1/cluster_info"));
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/merge_decision_1/adj_cluster"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/merge_decision_1/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/compare/avgbc/merge_decision_1/result",
      "testdata/compare/avgbc/merge_decision_1/output/part-r-00000"))

  }

  @Test
  def testMergeStep2: Unit = {
    var conf = new Configuration();
    var fs = FileSystem.get(conf);
    fs.delete(new Path("testdata/compare/avgbc/merge_decision_2/output"), true);

    var job = Job.getInstance(conf, "Raw Merge Right");
    job.setMapperClass(classOf[AdjClusterRightJoinMapper]);
    job.setReducerClass(classOf[AdjClusterRightJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/merge_decision_2/adj_cluster_left"));
    FileInputFormat.addInputPath(job, new Path("testdata/compare/avgbc/merge_decision_2/cluster_info"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/compare/avgbc/merge_decision_2/output"))
    job.waitForCompletion(true);
    assertTrue(FileCompare.compare("testdata/compare/avgbc/merge_decision_2/result",
      "testdata/compare/avgbc/merge_decision_2/output/part-r-00000"))
  }
}