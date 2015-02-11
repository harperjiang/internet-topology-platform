package edu.clarkson.cs.itop.tool.partition.geo

import edu.clarkson.cs.itop.tool.common.JoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text

class NodeGeoJoinMapper extends SingleKeyJoinMapper("node_geo", "geo_partition", 1, 0) {

}

class NodeGeoJoinReducer extends JoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    (new Text(left(0).toString), new Text(right(1).toString));
  }) {}