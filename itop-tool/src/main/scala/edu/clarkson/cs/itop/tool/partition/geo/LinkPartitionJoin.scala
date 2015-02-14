package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import scala.collection.JavaConversions._
import org.apache.hadoop.io.Writable
import java.util.Arrays
import edu.clarkson.cs.itop.tool.common.JoinReducer

/**
 * Input: node_partition (node_id,partition_id)
 * Input: node_link (link_id, node_id)
 * Output: link_id partition_id
 */

class LinkPartitionJoinMapper extends SingleKeyJoinMapper("node_partition", "node_link", 0, 1) {

}

class LinkPartitionJoinReducer extends JoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    (new Text(right(0).toString()), new Text(left(1).toString()))
  }) {
}