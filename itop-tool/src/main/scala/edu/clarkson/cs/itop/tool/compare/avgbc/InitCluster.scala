package edu.clarkson.cs.itop.tool.compare.avgbc

import org.apache.hadoop.io.IntWritable
import scala.collection.JavaConversions._
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.mutable.ArrayBuffer

/**
 * Input: node_id, degree
 * Output: cluster_id, node_id
 */
class InitClusterNodeMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var parts = value.toString().split("\\s+");
    var v = new IntWritable(parts(0).toInt);
    context.write(v, v);
  }
}

/**
 * Input:  node_id, partition_id
 * Output: cluster_id 1 partition_ids
 */
class InitClusterInfoMapper extends Mapper[Object, Text, IntWritable, IntWritable] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var parts = value.toString.split("\\s+")
    context.write(new IntWritable(parts(0).toInt), new IntWritable(parts(1).toInt));
  }
}

class InitClusterInfoReducer extends Reducer[IntWritable, IntWritable, IntWritable, Text] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, Text]#Context) = {
    var buffer = new ArrayBuffer[Int]();
    values.foreach { value =>
      {
        buffer += value.get
      }
    };
    context.write(key, new Text(Array("1", buffer.mkString(",")).mkString("\t")));
  }
}
