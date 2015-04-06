package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

import edu.clarkson.cs.itop.tool.common.JoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import scala.collection.JavaConversions._

/**
 * Input: adj_node (node_a, node_b)
 * Output: adj_node_dup (node_a, node_b)
 *                      (node_b, node_a)
 */
class AdjDuplicateMapper extends Mapper[Object, Text, Text, Text] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context) = {
    var parts = value.toString().split("\\s+");
    var a = new Text(parts(0));
    var b = new Text(parts(1));
    context.write(a, b);
    context.write(b, a);
  }
}

/**
 * Input: adj_node (node_a, node_b)
 * Input: triple (node_id, core_id, degree, length)
 * Output: adj_node_join (node_a, node_b, node_a_core, node_a_degree, node_a_length + 1)
 */
class AdjTripleJoinMapper extends SingleKeyJoinMapper("adj_node_dup", "triple", 0, 0);

class AdjTripleJoinReducer extends JoinReducer(null,
  (key, left, right) => {
    (key, new Text(Array(left(1).toString, right(1).toString, right(2).toString, (right(3).toString.toInt + 1).toString).mkString("\t")));
  });

/**
 * Input: adj_node_join (node_a, node_b, node_a_core, node_a_degree, node_a_length + 1)
 * Output: new_triple (node_b, node_a_core, max(node_a_degree), node_a_length + 1)
 */
class AdjTripleUpdateMapper extends Mapper[Object, Text, Text, StringArrayWritable] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, StringArrayWritable]#Context) = {
    var parts = value.toString.split("\\s+");
    context.write(new Text(parts(1)), new StringArrayWritable(Array(parts(2), parts(3), parts(4))));
  }
}

class AdjTripleUpdateReducer extends Reducer[Text, StringArrayWritable, Text, Text] {
  override def reduce(key: Text, values: java.lang.Iterable[StringArrayWritable], context: Reducer[Text, StringArrayWritable, Text, Text]#Context) = {
    var maxData: Array[String] = null;
    var maxDegree = -1;
    values.foreach(value => {
      var degree = value.toStrings()(1).toInt;
      if (degree > maxDegree) {
        maxData = value.toStrings();
        maxDegree = degree;
      }
    });
    context.write(key, new Text(Array(maxData(0), maxDegree.toString, maxData(2)).mkString("\t")))
  }
}