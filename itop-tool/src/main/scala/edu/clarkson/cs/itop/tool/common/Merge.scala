package edu.clarkson.cs.itop.tool.common

import org.apache.hadoop.io.IntWritable
import edu.clarkson.cs.itop.core.model.Link
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import edu.clarkson.cs.itop.core.parser.Parser
import org.apache.hadoop.io.NullWritable

class MergeMapper extends Mapper[Object, Text, Text, NullWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, NullWritable]#Context): Unit = {
    context.write(value, NullWritable.get);
  }
}