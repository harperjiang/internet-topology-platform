package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Assert.assertTrue
import org.junit.Test
import edu.clarkson.cs.itop.tool.FileCompare
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable

class GeoPartitionTest {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/geo/partition/output"), true);

    var job = Job.getInstance(conf, "Geo Partition");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[GeoPartitionMapper]);
    job.setReducerClass(classOf[GeoPartitionReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[NullWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    job.setNumReduceTasks(1);
    job.setGroupingComparatorClass(classOf[GeoPartitionGroupComparator]);

    FileInputFormat.addInputPath(job, new Path("testdata/geo/partition/input"));
    FileOutputFormat.setOutputPath(job, new Path("testdata/geo/partition/output"));
    job.waitForCompletion(true);

    assertTrue(FileCompare.compareContent("testdata/geo/partition/result", "testdata/geo/partition/output/part-r-00000"))

  }
}