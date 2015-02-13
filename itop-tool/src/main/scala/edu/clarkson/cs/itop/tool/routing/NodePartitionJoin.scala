package edu.clarkson.cs.itop.tool.routing

import java.util.Arrays

import scala.io.Source

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable

import edu.clarkson.cs.itop.tool.common.JoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper

/**
 * Input:
 *          link_id, node_id (node_link)
 *          link_id, partition_id (link_partition)
 * Output:
 *          node_id, partition_id
 */
class NodePartitionJoinMapper extends SingleKeyJoinMapper("node_link", "link_partition", 0, 0) {

}

class NodePartitionJoinReducer extends JoinReducer(null, (key: Text, left: Array[Writable], right: Array[Writable]) => {
  (new Text(left(1).toString()), new Text(right(1).toString()))
}) {}

