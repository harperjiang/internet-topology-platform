package edu.clarkson.cs.itop.tool.partition.exp

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

/**
 * Input: from_cluster to_cluster to_degree
 * Output: from_cluster to_cluster to_degree ( with only max degree )
 */
class RemoveTwoHeadMergeDecisionMapper extends Mapper[Object, Text, Text, Text] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context) = {
    var parts = value.toString().split("\\s+")
    context.write(new Text(parts(0)), new Text(Array(parts(1), parts(2)).mkString("\t")));
  }
}

class RemoveTwoHeadMergeDecisionReducer extends Reducer[Text, Text, Text, Text] {
  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context) = {
    var maxDegree = 0
    var maxContent: Text = null;
    values.foreach(value => {
      var parts = value.toString().split("\\s+")
      var degree = parts(1).toInt
      if (degree > maxDegree) {
        maxDegree = degree;
        maxContent = new Text(value.toString());
      }
    });
    context.write(key, maxContent)
  }
}

/**
 * Input: from_cluster to_cluster to_degree
 * Output: from_cluster to_cluster to_degree (only the header link)
 */
class ExtractHeaderMergeDecisionMapper extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context) = {
    var parts = value.toString().split("\\s+")
    context.write(new StringArrayWritable(Array(parts(1), "1")), new StringArrayWritable(Array("1", parts(0), parts(2))));
    context.write(new StringArrayWritable(Array(parts(0), "0")), new StringArrayWritable(Array("0", parts(1), parts(2))));
  }
}

class ExtractHeaderMergeDecisionReducer extends Reducer[StringArrayWritable, StringArrayWritable, Text, Text] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, Text, Text]#Context): Unit = {
    values.foreach(value => {
      var parts = value.toStrings
      parts(0) match {
        case "0" => {
          return ;
        }
        case "1" => {
          var to = key.toStrings()(0)
          var parts = value.toStrings()
          var from = parts(1)
          var toDegree = parts(2)
          context.write(new Text(Array(from, to).mkString("\t")), new Text(toDegree))
        }
      }
    })
  }
}