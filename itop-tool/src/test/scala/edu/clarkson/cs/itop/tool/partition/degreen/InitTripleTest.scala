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
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path("testdata/degreen/init_triple/node_degree"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/degreen/init_triple/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degree/join_link/result", "testdata/degree/join_link/output/part-r-00000"))
  }
}