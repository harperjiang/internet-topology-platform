package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

import edu.clarkson.cs.itop.tool.Param
import edu.clarkson.cs.itop.tool.types.EnhancedKeyGroupComparator
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

/**
 * Should go together with EnhancedKeyGrouper and sort with 2nd key
 */

class GeoPartitionMapper extends Mapper[Object, Text, StringArrayWritable, NullWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, NullWritable]#Context): Unit = {
    var values = value.toString().split("\\s+")
    context.write(new StringArrayWritable(values), NullWritable.get)
  }
}

class GeoPartitionReducer extends Reducer[StringArrayWritable, NullWritable, Text, IntWritable] {

  var sum = new Array[Int](Param.partition_count);

  {
    for (i <- 0 to Param.partition_count - 1) {
      sum(i) = 0;
    }
  }

  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[NullWritable],
    context: Reducer[StringArrayWritable, NullWritable, Text, IntWritable]#Context) = {
    var code = key.get()(0).toString()
    var count = key.get()(1).toString().toInt
    var si = smallestIndex
    sum(si) += count
    context.write(new Text(code), new IntWritable(si))
  }

  def smallestIndex: Int = {
    var smallest = Integer.MAX_VALUE
    var index = 0
    for (i <- 0 to sum.length - 1) {
      if (sum(i) < smallest) {
        smallest = sum(i)
        index = i
      }
    }
    return index;
  }
}

class GeoPartitionGroupComparator extends EnhancedKeyGroupComparator(Array(null, false)) {

}
