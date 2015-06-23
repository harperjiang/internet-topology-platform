package edu.clarkson.cs.itop.tool.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Assert.assertTrue
import org.junit.Test

import edu.clarkson.cs.itop.tool.FileCompare
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

class JoinTest {

  @Test
  def testInnerJoin: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/common/join/inner_output"), true);

    var icjob1 = Job.getInstance(conf, "Inner Join");
    icjob1.setJarByClass(classOf[JoinTest]);
    icjob1.setMapperClass(classOf[JoinTestMapper]);
    icjob1.setReducerClass(classOf[InnerJoinTestReducer]);
    icjob1.setMapOutputKeyClass(classOf[StringArrayWritable]);
    icjob1.setMapOutputValueClass(classOf[StringArrayWritable]);
    icjob1.setOutputKeyClass(classOf[IntWritable]);
    icjob1.setOutputValueClass(classOf[Text]);
    icjob1.setPartitionerClass(classOf[KeyPartitioner]);
    icjob1.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(icjob1, new Path("testdata/common/join/left"));
    FileInputFormat.addInputPath(icjob1, new Path("testdata/common/join/right"));
    FileOutputFormat.setOutputPath(icjob1, new Path("testdata/common/join/inner_output"));
    icjob1.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/common/join/inner_result", "testdata/common/join/inner_output/part-r-00000"))

  }

  @Test
  def testLeftJoin: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/common/join/left_output"), true);

    var icjob1 = Job.getInstance(conf, "left Join");
    icjob1.setJarByClass(classOf[JoinTest]);
    icjob1.setMapperClass(classOf[JoinTestMapper]);
    icjob1.setReducerClass(classOf[LeftJoinTestReducer]);
    icjob1.setMapOutputKeyClass(classOf[StringArrayWritable]);
    icjob1.setMapOutputValueClass(classOf[StringArrayWritable]);
    icjob1.setOutputKeyClass(classOf[IntWritable]);
    icjob1.setOutputValueClass(classOf[Text]);
    icjob1.setPartitionerClass(classOf[KeyPartitioner]);
    icjob1.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(icjob1, new Path("testdata/common/join/left"));
    FileInputFormat.addInputPath(icjob1, new Path("testdata/common/join/right"));
    FileOutputFormat.setOutputPath(icjob1, new Path("testdata/common/join/left_output"));
    icjob1.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/common/join/left_result", "testdata/common/join/left_output/part-r-00000"))

  }

  @Test
  def testRightJoin: Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    // true stands for recursively deleting the folder you gave
    fs.delete(new Path("testdata/common/join/right_output"), true);

    var icjob1 = Job.getInstance(conf, "Right Join");
    icjob1.setJarByClass(classOf[JoinTest]);
    icjob1.setMapperClass(classOf[JoinTestMapper]);
    icjob1.setReducerClass(classOf[RightJoinTestReducer]);
    icjob1.setMapOutputKeyClass(classOf[StringArrayWritable]);
    icjob1.setMapOutputValueClass(classOf[StringArrayWritable]);
    icjob1.setOutputKeyClass(classOf[IntWritable]);
    icjob1.setOutputValueClass(classOf[Text]);
    icjob1.setPartitionerClass(classOf[KeyPartitioner]);
    icjob1.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(icjob1, new Path("testdata/common/join/left"));
    FileInputFormat.addInputPath(icjob1, new Path("testdata/common/join/right"));
    FileOutputFormat.setOutputPath(icjob1, new Path("testdata/common/join/right_output"));
    icjob1.waitForCompletion(true);

    assertTrue(FileCompare.compare("testdata/common/join/right_result", "testdata/common/join/right_output/part-r-00000"))

  }
}

class JoinTestMapper extends SingleKeyJoinMapper("left", "right", 0, 0) {

}

class InnerJoinTestReducer extends JoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => { (new Text(left(1).toString()), new Text(right(1).toString())) }) {

}

class LeftJoinTestReducer extends LeftOuterJoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    if (right == null) {
      (new Text(left(1).toString()), new Text("-1"))
    } else { (new Text(left(1).toString()), new Text(right(1).toString())) }
  }) {

}

class RightJoinTestReducer extends RightOuterJoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    if (left == null) {
      (new Text("-1"), new Text(right(1).toString()))
    } else {
      (new Text(left(1).toString()), new Text(right(1).toString()))
    }
  }) {
}