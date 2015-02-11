package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import scala.collection.JavaConversions._
import org.apache.hadoop.io.Writable
import java.util.Arrays
import edu.clarkson.cs.itop.tool.common.JoinReducer

class LinkPartitionMapper extends SingleKeyJoinMapper("node_link", "node_partition", 1, 0) {

}

class LinkPartitionReducer extends JoinReducer(null, 
    (key: Text, left: Array[Writable], right: Array[Writable]) => {
      (new Text(left(0).toString()),new Text(right(1).toString()))
    }) {
}