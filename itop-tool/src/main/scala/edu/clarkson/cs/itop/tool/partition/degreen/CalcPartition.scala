package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import edu.clarkson.cs.itop.tool.common.JoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import scala.collection.JavaConversions._
import edu.clarkson.cs.itop.tool.Param

/**
 * Input: triple (node_id, core_id, core_degree, core_length)
 * Input: node_link (link_id, node_id)
 * Output: triple_link_join (link_id, core_id, core_degree)
 */
class JoinTripleLinkMapper extends SingleKeyJoinMapper("triple", "node_link", 0, 1);

class JoinTripleLinkReducer extends JoinReducer(null, (key, left, right) => {
  (new Text(right(0).toString()), new Text(Array(left(1).toString, left(2).toString).mkString("\t")))
});

/**
 * Input: triple_join (link_id, core_id, core_degree)
 * Output:link_id partition_id
 */
class AssignLinkToPartitionMapper extends Mapper[Object, Text, IntWritable, Text] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context) = {
    var parts = value.toString().split("\\s+");
    context.write(new IntWritable(parts(0).toInt), new Text(Array(parts(1), parts(2)).mkString("\t")))
  }
}

class AssignLinkToPartitionReducer extends Reducer[IntWritable, Text, IntWritable, IntWritable] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[Text], context: Reducer[IntWritable, Text, IntWritable, IntWritable]#Context) = {
    var maxData: Array[String] = null;
    var maxDegree = -1;
    values.foreach(value => {
      var data = value.toString().split("\\s+");
      var degree = data(1).toInt;
      if (degree > maxDegree) {
        maxDegree = degree;
        maxData = data;
      }
    });
    var maxCoreId = maxData(0);
    var partition = maxCoreId.hashCode() % Param.partition_count;

    context.write(key, new IntWritable(partition));
  }
}