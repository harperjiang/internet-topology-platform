package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import scala.collection.JavaConversions._
import edu.clarkson.cs.itop.tool.Param

/**
 * Input: link_id, partition_id
 * Output: link_id, partition_id (choose the majority partition)
 *
 * Partition id = -1 means unknown. If a link has only unknown partition, it will be randomly distributed to some partitions
 */

class LinkAssignMapper extends Mapper[Object, Text, IntWritable, IntWritable] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var values = value.toString().split("\\s+")
    context.write(new IntWritable(values(0).toInt), new IntWritable(values(1).toInt));
  }
}

class LinkAssignReducer extends Reducer[IntWritable, IntWritable, IntWritable, IntWritable] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, IntWritable]#Context) = {
    var counter = new scala.collection.mutable.HashMap[Int, Int]();
    values.foreach(value => {
      if (!counter.contains(value.get())) {
        counter += { value.get -> 0 }
      }
      counter += { value.get -> (counter.get(value.get).get + 1) }
    })

    if (counter.size == 1 && counter.contains(-1)) {
      // Random Distribution
      context.write(key, new IntWritable(Math.abs(key.hashCode()) % Param.partition_count));
    } else {
      counter.remove(-1);
      var bigCount = 0;
      var bigPartition = -1;

      counter.foreach(entry => {
        if (entry._2 > bigCount) {
          bigCount = entry._2;
          bigPartition = entry._1;
        }
      })
      context.write(key, new IntWritable(bigPartition))
    }
  }
}