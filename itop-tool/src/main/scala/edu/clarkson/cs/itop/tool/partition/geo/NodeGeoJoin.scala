package edu.clarkson.cs.itop.tool.partition.geo

import edu.clarkson.cs.itop.tool.common.JoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text
/**
 * Input: node_geo (node_id, geo_code)
 * Input: geo_partition (geo_code, partition)
 * Output: gnode_partition (node_id, partition_id)
 */

class NodeGeoJoinMapper extends SingleKeyJoinMapper("geo_partition", "node_geo", 0, 1) {

}

class NodeGeoJoinReducer extends JoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    (new Text(right(0).toString), new Text(left(1).toString));
  }) {}