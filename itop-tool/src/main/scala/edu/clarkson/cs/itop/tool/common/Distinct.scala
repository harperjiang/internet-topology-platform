package edu.clarkson.cs.itop.tool.common

import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.NullWritable

/**
 *  Input: something
 *  Output:  (distinct)
 */
class DistinctMapper extends Mapper[Object, Text, StringArrayWritable, Text] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, Text]#Context): Unit = {
    context.write(new StringArrayWritable(value.toString().split("\\s")), new Text(""));
  }
}

class DistinctReducer extends Reducer[StringArrayWritable, Text, Text, NullWritable] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[Text],
    context: Reducer[StringArrayWritable, Text, Text, NullWritable]#Context): Unit = {
    context.write(new Text(key.get().map { a => a.toString() }.mkString(" ")), NullWritable.get);
  }
}