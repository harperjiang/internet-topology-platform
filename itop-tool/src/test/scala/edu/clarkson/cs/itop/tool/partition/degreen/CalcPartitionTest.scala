package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Test
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.FileSystem
import org.junit.Assert._

class CalcPartitionTest {

  @Test
  def testJoinTripleLink(): Unit = {
    var conf = new Configuration();
    FileSystem.get(conf).delete(new Path("testdata/degreen/join_triple_link/output"), true);

    var job = Job.getInstance(conf, "Degree - Join Triple Link");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[JoinTripleLinkMapper]);
    job.setReducerClass(classOf[JoinTripleLinkReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/join_triple_link/triple"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/join_triple_link/node_link"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/join_triple_link/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/join_triple_link/result", "testdata/degreen/join_triple_link/output/part-r-00000"))
  }

  @Test
  def testAssignLinkToPartition(): Unit = {
    var conf = new Configuration();
    FileSystem.get(conf).delete(new Path("testdata/degreen/assign_link_partition/output"), true);
    var job = Job.getInstance(conf, "Degree - Assign Link to Partition");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AssignLinkToPartitionMapper]);
    job.setReducerClass(classOf[AssignLinkToPartitionReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/assign_link_partition/triple_link_join"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/assign_link_partition/output"));
    job.waitForCompletion(true);
    assertTrue(FileCompare.compare("testdata/degreen/assign_link_partition/result", "testdata/degreen/assign_link_partition/output/part-r-00000"))
  }
}