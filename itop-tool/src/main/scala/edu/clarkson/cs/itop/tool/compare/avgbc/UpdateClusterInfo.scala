package edu.clarkson.cs.itop.tool.compare.avgbc

import edu.clarkson.cs.itop.tool.common.RightOuterJoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text

/**
 * Input: from_cluster to_cluster from_size (merge_decision)
 * Input: cluster_id partition_id cluster_size (cluster_info)
 * Output: cluster_id partition_id cluster_size (remove from, add from_size to to_size)
 */

/**
 * Input: from_cluster to_cluster from_size(merge_decision)
 * Input: cluster_info
 * Output: cluster_info - from_cluster
 */

class RemoveFromMapper extends SingleKeyJoinMapper("merge_decision", "cluster_info", 0, 0);

class RemoveFromReducer extends RightOuterJoinReducer(
  (key: Text, left: Array[Writable], right: Array[Writable]) => { left == null },
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    (new Text(right(0).toString()), new Text((Array(right(1).toString(), right(2).toString)).mkString("\t")))
  });

/**
 * Input: from_cluster to_cluster from_size (merge_decision)
 * Input: cluster_info - from
 * Output: cluster_info size_added
 */
class AddToMapper extends SingleKeyJoinMapper("merge_decision", "cluster_info", 1, 0);

class AddToReducer extends RightOuterJoinReducer(
  null,
  (key, left, right) => {
    if (left == null) {
      (new Text(right(0).toString()), new Text(Array(right(1).toString, right(2).toString).mkString("\t")));
    } else {
      var oldSize = right(2).toString.toInt;
      var addedSize = left(2).toString.toInt;
      var newSize = oldSize + addedSize;
      (new Text(right(0).toString), new Text(Array(right(1).toString, newSize.toString).mkString("\t")));
    }
  });