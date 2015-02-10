package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

/**
 * Get geo information
 * Input: nodes.geo
 * Output: node_id geo_code(2 char)
 */

class NodeGeoMapper extends Mapper[Object, Text, IntWritable, Text] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
    if (value.toString().startsWith("#"))
      return ;
    var data = value.toString().split("\\s+");
    var id = data(1).substring(1, data(1).length() - 1).toInt;
    context.write(new IntWritable(id), new Text(data(3)));
  }
}