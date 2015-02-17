package edu.clarkson.cs.itop.tool.partition.degreen

import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import edu.clarkson.cs.itop.tool.common.JoinReducer
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable

/**
 * Input: cluster_id, cluster_id (adj_cluster)
 * Input: cluster_id merge_times, degree (cluster)
 * Output: cluster_id cluster_id left_degree
 */
class MergeDecisionLeftDegreeMapper extends SingleKeyJoinMapper("cluster", "adj_cluster", 0, 0) {

}

class MergeDecisionLeftDegreeReducer extends JoinReducer(null, (key: Text, left: Array[Writable], right: Array[Writable]) => {
  (key, new Text(Array(left(1).toString, right(2).toString).mkString("\t")))
}) {

}
/**
 * Input: cluster_id_1, cluster_id_2 left_degree
 * Input: cluster_id_2 merge_times, degree (cluster)
 * Output: cluster_id_1 cluster_id_2 left_degree right_degree
 */
class MergeDecisionRightDegreeMapper extends SingleKeyJoinMapper("cluster", "adj_cluster_left", 0, 1) {

}

class MergeDecisionRightDegreeReducer extends JoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    (new Text(Array(right(0).toString(), right(1).toString()).mkString("\t")),
      new Text(Array(right(2).toString, left(2).toString).mkString("\t")))
  }) {

}

/**
 * Input: cluster_id_left, cluster_id_right, left_degree, right_degree
 * Output: from_cluster to_cluster to_degree
 */
class MergeDecisionMapper extends Mapper[Object, Text, IntWritable, Text] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context) = {
    var parts = value.toString().split("\\s+");
    var leftId = parts(0).toInt
    var rightId = parts(1).toInt
    var leftDegree = parts(2).toInt
    var rightDegree = parts(3).toInt
    if (leftDegree > rightDegree) {
      context.write(new IntWritable(rightId), new Text(Array(leftId.toString, leftDegree.toString).mkString("\t")))
    }
    if (rightDegree > leftDegree) {
      context.write(new IntWritable(leftId), new Text(Array(rightId.toString, rightDegree.toString).mkString("\t")))
    }
  }
}