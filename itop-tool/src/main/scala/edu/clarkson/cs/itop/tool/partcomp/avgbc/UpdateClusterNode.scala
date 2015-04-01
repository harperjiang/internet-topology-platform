package edu.clarkson.cs.itop.tool.partcomp.avgbc

import edu.clarkson.cs.itop.tool.common.RightOuterJoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text

/**
 * Input: from_cluster, to_cluster from_size (merge_decision)
 * Input: cluster_id, node_id (cluster_node)
 * Output: cluster_id, node_id ( all from_cluster replaced by to_cluster)
 */
class UpdateClusterNodeMapper extends SingleKeyJoinMapper("merge_decision", "cluster_node", 0, 0) {

}

class UpdateClusterNodeReducer extends RightOuterJoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    if (left == null) {
      (new Text(right(0).toString), new Text(right(1).toString))
    } else {
      (new Text(left(1).toString), new Text(right(1).toString))
    }
  })

