package edu.clarkson.cs.itop.tool.partition.exp

import org.apache.hadoop.mapreduce.Mapper
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import edu.clarkson.cs.itop.tool.Utils
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import edu.clarkson.cs.itop.tool.common.RightOuterJoinReducer
import org.apache.hadoop.io.Writable

/**
 * Input: cluster_id, max_degree(cluster)
 * Input: cluster_from, cluster_to(merge_decision)
 * Output: if cluster_id is a from, remove it
 */

class UpdateClusterMapper extends SingleKeyJoinMapper("merge_decision", "cluster", 0, 0)

class UpdateClusterReducer extends RightOuterJoinReducer(
  (key: Text, left: Array[Writable], right: Array[Writable]) => { left == null },
  (key: Text, left: Array[Writable], right: Array[Writable]) => { (new Text(right(0).toString()), new Text(right(1).toString())) })