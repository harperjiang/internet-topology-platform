package edu.clarkson.cs.itop.tool.common

import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper

/**
 *  Input: something
 *  Output:  (distinct)
 */
class DistinctMapper extends Mapper[Object, Text, StringArrayWritable, Text] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, Text]#Context): Unit = {
    context.write(new StringArrayWritable(value.toString().split("\\s")), null);
  }
}

class DistinctReducer extends Reducer[StringArrayWritable, Text, StringArrayWritable, Text] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[Text],
    context: Reducer[StringArrayWritable, Text, StringArrayWritable, Text]#Context): Unit = {
    context.write(key, null);
  }
}