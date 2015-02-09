package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.IntWritable

class GeoPartitionMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
    if (value.toString().startsWith("#"))
      return ;
    var data = value.toString().split("\\s+");
    var id = data(1).substring(1, data(1).length() - 1).toInt;
    var partition = CountryCode.continent(data(3));
    context.write(new IntWritable(id), new IntWritable(partition));
  }
}