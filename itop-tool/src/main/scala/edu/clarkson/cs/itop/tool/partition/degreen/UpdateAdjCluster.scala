package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable

import edu.clarkson.cs.itop.tool.common.RightOuterJoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper

/**
 * Input: small_cluster, big_cluster (adj_cluster)
 * Input: from_cluster, to_cluster (merge_decision)
 * Output: small_cluster, big_cluster (all appearance of from_cluster should be replaced by to_cluster)
 */

/**
 * Input: from_cluster, to_cluster(merge_decision)
 * Input: small_cluster, big_cluster (adj_cluster)
 * Output: small_replaced_cluster, big_cluster (adj_cluster_left)
 */
class UpdateLeftAdjClusterMapper extends SingleKeyJoinMapper("merge_decision", "adj_cluster", 0, 0)

class UpdateLeftAdjClusterReducer extends RightOuterJoinReducer(
  null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    var to = right(1).toString.toInt
    var from = -1
    if (left == null)
      from = right(0).toString.toInt
    else
      from = left(1).toString.toInt
      
    if (from == to)
      null;
    else
      (new Text(from.toString), new Text(to.toString))
  })

/**
 * Input: from_cluster, to_cluster(merge_decision)
 * Input: small_replaced_cluster, big_cluster (adj_cluster_updated_left)
 * Output: small_replaced_cluster, big_replaced_cluster (adj_cluster_dup)
 */
class UpdateRightAdjClusterMapper extends SingleKeyJoinMapper("merge_decision", "adj_cluster_updated_left", 0, 1)

class UpdateRightAdjClusterReducer extends RightOuterJoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    var from = right(0).toString.toInt
    var to = -1
    if (left == null) {
      to = right(1).toString.toInt
    } else {
      to = left(1).toString.toInt
    }
    if (from == to)
      null;
    else
      (new Text(Math.min(from, to).toString), new Text(Math.max(from, to).toString))
  })