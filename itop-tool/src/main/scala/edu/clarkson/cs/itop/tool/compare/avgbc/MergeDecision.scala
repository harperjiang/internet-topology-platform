package edu.clarkson.cs.itop.tool.compare.avgbc

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.WritableComparator
import org.apache.hadoop.io.WritableComparable
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import edu.clarkson.cs.itop.tool.common.JoinReducer
import org.apache.hadoop.io.Writable

/**
 * Input : adj_cluster (cluster_a cluster_b)
 * Input : cluster_info (cluster_id size partition_id)
 * Output: cluster_a cluster_b a_size a_partition_id
 */

class AdjClusterLeftJoinMapper extends SingleKeyJoinMapper("cluster_info", "adj_cluster", 0, 0);

class AdjClusterLeftJoinReducer extends JoinReducer(null, (key: Text, left: Array[Writable], right: Array[Writable]) => {
  (key, new Text(Array(right(1).toString(), left(1).toString(), left(2).toString()).mkString("\t")))
});

/**
 * Input:  cluster_info (cluster_id size partition_id)
 * Input:  adj_cluster_left (cluster_a cluster_b a_size a_partition_id)
 * Output: cluster_merge_from cluster_merge_to from_size to_size)
 * Notes: always merge larger id to smaller id
 */
class AdjClusterRightJoinMapper extends SingleKeyJoinMapper("cluster_info", "adj_cluster_left", 0, 1);

class AdjClusterRightJoinReducer extends JoinReducer(
  (key, left, right) => {
    var leftpart = right(3).toString;
    var rightpart = left(2).toString;
    if (leftpart.contains(",") || rightpart.contains(",")) { // Ignore nodes with multiple partitions
      false;
    } else {
      leftpart.equals(rightpart);
    }
  },
  (key, left, right) => {
    var aid = right(0).toString.toInt;
    var bid = key.toString.toInt;
    if (aid < bid) {
      (new Text(bid.toString), new Text(Array(aid.toString, left(1).toString, right(2).toString).mkString("\t")));
    } else {
      (new Text(aid.toString), new Text(Array(bid.toString, right(2).toString, left(1).toString).mkString("\t")));
    }
  });