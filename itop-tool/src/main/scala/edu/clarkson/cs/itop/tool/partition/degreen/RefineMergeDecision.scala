package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._

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
      if (degree > maxDegree)
        maxContent = value
    });
    context.write(key, maxContent)
  }
}

/**
 * Input: from_cluster to_cluster to_degree
 * Output: from_cluster to_cluster to_degree (only the header link)
 */
class ExtractHeaderMergeDecisionMapper extends Mapper[Object, Text, Text, Text] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context) = {
    var parts = value.toString().split("\\s+")
    context.write(new Text(Array(parts(1), "1").mkString("\t")), new Text(Array("1", parts(0), parts(2)).mkString("\t")));
    context.write(new Text(Array(parts(0), "0").mkString("\t")), new Text(Array("0", parts(1), parts(2)).mkString("\t")));
  }
}

class ExtractHeaderMergeDecisionReducer extends Reducer[Text, Text, Text, Text] {
  override def reduce(key: Text, values: java.lang.Iterable[Text],
    context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    values.foreach(value => {
      var parts = value.toString().split("\\s+")
      parts(0) match {
        case "0" => {
          return ;
        }
        case "1" => {
          var to = key.toString().split("\\s+")(0)
          var parts = value.toString().split("\\s+")
          var from = parts(1)
          var toDegree = parts(2)
          context.write(new Text(Array(from,to).mkString("\t")),new Text(toDegree))
        }
      }
    })
  }
}