package edu.clarkson.cs.itop.tool.partition.degree

import org.junit.Test
import org.junit.Assert._
import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class MaxDegreeTest {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/degree/max_degree/output"), true);

    var icjob2 = Job.getInstance(conf, "Degree I1 - Max Degree");
    icjob2.setJarByClass(classOf[MaxDegreeTest]);
    icjob2.setMapperClass(classOf[MaxDegreeMapper]);
    icjob2.setReducerClass(classOf[MaxDegreeReducer]);
    icjob2.setOutputKeyClass(classOf[IntWritable]);
    icjob2.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(icjob2, new Path("testdata/degree/max_degree/input"))
    FileOutputFormat.setOutputPath(icjob2, new Path("testdata/degree/max_degree/output"));
    icjob2.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/degree/max_degree/result", "testdata/degree/max_degree/output/part-r-00000"))
  }
}