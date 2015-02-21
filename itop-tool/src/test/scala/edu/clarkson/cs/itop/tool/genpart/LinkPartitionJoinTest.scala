package edu.clarkson.cs.itop.tool.genpart

import org.junit.Test
import edu.clarkson.cs.itop.tool.FileCompare
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.partition.degree.JoinLinkDegreeMapper
import edu.clarkson.cs.itop.tool.partition.degree.JoinLinkDegreeReducer
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.partition.degree.Main1
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.io.Text
import org.junit.Assert._

class LinkPartitionJoinTest {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/genpart/link_partition_join/output"), true);

    var icjob1 = Job.getInstance(conf, "Link Partition Join");
    icjob1.setJarByClass(Main1.getClass);
    icjob1.setMapperClass(classOf[LinkPartitionJoinMapper]);
    icjob1.setReducerClass(classOf[LinkPartitionJoinReducer]);
    icjob1.setMapOutputKeyClass(classOf[StringArrayWritable]);
    icjob1.setMapOutputValueClass(classOf[StringArrayWritable]);
    icjob1.setOutputKeyClass(classOf[Text]);
    icjob1.setOutputValueClass(classOf[Text]);
    icjob1.setPartitionerClass(classOf[KeyPartitioner]);
    icjob1.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(icjob1, new Path("testdata/genpart/link_partition_join/link_partition"));
    FileInputFormat.addInputPath(icjob1, new Path("testdata/genpart/link_partition_join/kapar-midar-iff.links"));
    FileOutputFormat.setOutputPath(icjob1, new Path("testdata/genpart/link_partition_join/output"));
    icjob1.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/genpart/link_partition_join/result", "testdata/genpart/link_partition_join/output/part-r-00000"))
  }
}