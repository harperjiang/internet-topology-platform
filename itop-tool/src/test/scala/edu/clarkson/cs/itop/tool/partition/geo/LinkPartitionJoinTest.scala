package edu.clarkson.cs.itop.tool.partition.geo

import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Test
import org.apache.hadoop.mapreduce.Job

class LinkPartitionTest {

  @Test
  def test: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/geo/link_partition_join/output"), true);
    
    var job = Job.getInstance(conf, "Link Partition Join");
    job.setMapperClass(classOf[LinkPartitionJoinMapper]);
    job.setReducerClass(classOf[LinkPartitionJoinReducer]);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    job.setPartitionerClass(classOf[KeyPartitioner]);
    job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(job, new Path("testdata/geo/link_partition_join/node_link"))
    FileInputFormat.addInputPath(job, new Path("testdata/geo/link_partition_join/node_partition"))
    FileOutputFormat.setOutputPath(job, new Path("testdata/geo/link_partition_join/output"))
    job.waitForCompletion(true);
  }
}