package edu.clarkson.cs.itop.tool.partition.degreen

import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.conf.Configuration
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.FileCompare
import org.junit.Test
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.partition.degreen.TripleDiffMapper
import edu.clarkson.cs.itop.tool.partition.degreen.TripleDiffReducer
import edu.clarkson.cs.itop.tool.partition.degreen.TripleUpdateMapper
import edu.clarkson.cs.itop.tool.partition.degreen.TripleUpdateReducer

class AdjTripleJoinTest {

  @Test
  def testAdjTripleJoin() = {
    var conf = new Configuration();
    FileSystem.get(conf).delete(new Path("testdata/degreen/adj_triple_join/output"), true);

    var job = Job.getInstance(conf, "Degree - Adj Tripple Join");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AdjTripleJoinMapper]);
    job.setReducerClass(classOf[AdjTripleJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/adj_triple_join/adj_node_dup"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/adj_triple_join/triple"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/adj_triple_join/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/adj_triple_join/result", "testdata/degreen/adj_triple_join/output/part-r-00000"))
  }

  @Test
  def testAdjTripleJoin2() = {
    var conf = new Configuration();
    FileSystem.get(conf).delete(new Path("testdata/degreen/adj_triple_join2/output"), true);

    var job = Job.getInstance(conf, "Degree - Adj Tripple Join");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AdjTripleJoinMapper]);
    job.setReducerClass(classOf[AdjTripleJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/adj_triple_join2/adj_node_dup"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/adj_triple_join2/triple"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/adj_triple_join2/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/adj_triple_join2/result", "testdata/degreen/adj_triple_join2/output/part-r-00000"))
  }

  @Test
  def testTripleDiff() = {
    var conf = new Configuration();
    FileSystem.get(conf).delete(new Path("testdata/degreen/triple_diff/output"), true);

    var job = Job.getInstance(conf, "Degree - Triple Diff");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[TripleDiffMapper]);
    job.setReducerClass(classOf[TripleDiffReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/triple_diff/triple_new"));
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/triple_diff/triple_old"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/triple_diff/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/triple_diff/result", "testdata/degreen/triple_diff/output/part-r-00000"))
  }

  @Test
  def testTripleUpdate() = {

    var conf = new Configuration();
    FileSystem.get(conf).delete(new Path("testdata/degreen/triple_update/output"), true);

    var job = Job.getInstance(conf, "Degree - Triple Update");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[TripleUpdateMapper]);
    job.setReducerClass(classOf[TripleUpdateReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/triple_update/adj_triple_join"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/triple_update/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/triple_update/result", "testdata/degreen/triple_update/output1/part-r-00000"))

  }
  
   @Test
  def testTripleUpdate2() = {

    var conf = new Configuration();
    FileSystem.get(conf).delete(new Path("testdata/degreen/triple_update2/output"), true);

    var job = Job.getInstance(conf, "Degree - Triple Update");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[TripleUpdateMapper]);
    job.setReducerClass(classOf[TripleUpdateReducer]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/triple_update2/adj_triple_join"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/triple_update2/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/triple_update2/result", "testdata/degreen/triple_update2/output/part-r-00000"))

  }
}