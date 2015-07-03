package edu.clarkson.cs.itop.tool.partition.degreen

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

import edu.clarkson.cs.itop.tool.common.JoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

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
 * Input: adj_node_dup (node_a, node_b)
 * Input: triple (node_id, core_id, degree, length)
 * Output: adj_node_join (node_a, node_b, node_a_core, node_a_degree, node_a_length + 1)
 */
class AdjTripleJoinMapper extends SingleKeyJoinMapper("adj_node_dup", "triple", 0, 0);

class AdjTripleJoinReducer extends JoinReducer(null,
  (key, left, right) => {
    (key, new Text(Array(left(1).toString, right(1).toString, right(2).toString, (right(3).toString.toInt + 1).toString).mkString("\t")));
  });

/**
 * Do we need to include triple_retain and old_triple in this round?
 * Original triple is already there
 * Old triples are generated from the same process, thus it will also win in this round
 */

/**
 * Input: adj_node_join (node_a, node_b, node_a_core, node_a_degree, new_length)
 * Output: new_triple (node_b, node_a_core, max(node_a_degree), min(new_length))
 */
class TripleUpdateMapper extends Mapper[Object, Text, IntWritable, StringArrayWritable] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, StringArrayWritable]#Context) = {
    var parts = value.toString.split("\\s+");
    context.write(new IntWritable(parts(1).toInt), new StringArrayWritable(Array(parts(2), parts(3), parts(4))));
  }
}

class TripleUpdateReducer extends Reducer[IntWritable, StringArrayWritable, IntWritable, Text] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[IntWritable, StringArrayWritable, IntWritable, Text]#Context) = {
    var maxData: Array[String] = null;
    var maxDegree = -1;
    var minLength = Integer.MAX_VALUE;
    values.foreach(value => {
      var parts = value.toStrings();
      var degree = parts(1).toInt;
      var length = parts(2).toInt;
      if (degree > maxDegree || (degree == maxDegree && length < minLength)) {
        maxData = value.toStrings();
        maxDegree = degree;
        minLength = length;
      }
    });
    context.write(key, new Text(Array(maxData(0), maxDegree.toString, minLength.toString).mkString("\t")))
  }
}

/**
 * Input: new_triple (node, core, degree, length)
 * Input: old_triple (node, core, degree, length)
 * Output: triple that doesn't change
 */
class TripleDiffMapper extends SingleKeyJoinMapper("triple_old", "triple_new", 0, 0);

class TripleDiffReducer extends JoinReducer(
  (key, left, right) => {
    var oldcore = left(1).toString;
    var newcore = right(1).toString;
    oldcore.equals(newcore)
  },
  (key, left, right) => {
    (key, new Text(Array(left(1), left(2), left(3)).map { _.toString }.mkString("\t")))
  });