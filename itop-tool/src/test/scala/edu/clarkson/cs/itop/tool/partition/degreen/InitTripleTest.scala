package edu.clarkson.cs.itop.tool.partition.degreen

import org.junit.Test
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.Text
import edu.clarkson.cs.itop.tool.FileCompare
import org.junit.Assert._
import org.apache.hadoop.io.IntWritable

class InitTripleTest {

  @Test
  def testInitTriple() = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degreen/init_triple/output"), true);

    var job = Job.getInstance(conf, "Degree - Init Triple");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[InitTripleMapper]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/init_triple/node_degree"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/init_triple/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/init_triple/result", "testdata/degreen/init_triple/output/part-r-00000"))
  }

  @Test
  def testInitTripleRetain() = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degreen/init_triple_retain/output"), true);

    var job = Job.getInstance(conf, "Degree - Init Triple Retain");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[InitTripleRetainMapper]);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/init_triple_retain/triple"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/init_triple_retain/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/init_triple_retain/result", "testdata/degreen/init_triple_retain/output/part-r-00000"))
  }

  @Test
  def testDuplicateAdjNode() = {
    var conf = new Configuration();
    FileSystem.get(conf).delete(new Path("testdata/degreen/adj_node_dup/output"), true);

    var job = Job.getInstance(conf, "Degree - Duplicate Adj Node");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[AdjDuplicateMapper]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/adj_node_dup/adj_node"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/adj_node_dup/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degreen/adj_node_dup/result", "testdata/degreen/adj_node_dup/output/part-r-00000"))
  }
}