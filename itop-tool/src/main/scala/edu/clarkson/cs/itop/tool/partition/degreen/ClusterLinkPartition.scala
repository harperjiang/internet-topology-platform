package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Mapper
import edu.clarkson.cs.itop.tool.common.JoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._
import edu.clarkson.cs.itop.tool.Param

/**
 * Input: cluster_id, node_id (cluster_node)
 * Input: cluster_id, merge_times, max_degree (cluster)
 * Input: link_id, node_id (node_link)
 * Output:link_id, partition_id (link_partition)
 */

/**
 * Input: cluster_id, max_degree (cluster)
 * Input: cluster_id, node_id (cluster_node)
 * Output: cluster_id node_id max_degree (cluster_node_degree)
 */
class ClusterDegreeMapper extends SingleKeyJoinMapper("cluster", "cluster_node", 0, 0) {

}

class ClusterDegreeReducer extends JoinReducer(null, (key: Text, left: Array[Writable], right: Array[Writable]) => {
  (key, new Text(Array(right(1).toString(), left(1).toString()).mkString("\t")))
})

/**
 * Input: link_id, node_id (node_link)
 * Input: cluster_id, node_id, max_degree (cluster_node_degree)
 * Output: link_id, cluster_id, cluster_degree (cluster_link)
 */
class ClusterLinkMapper extends SingleKeyJoinMapper("node_link", "cluster_node_degree", 1, 1) {

}

class ClusterLinkReducer extends JoinReducer(null, (key: Text, left: Array[Writable], right: Array[Writable]) => {
  (new Text(left(0).toString()), new Text(Array(right(0).toString(), right(2).toString()).mkString("\t")))
}) {

}

/**
 * Input: link_id, cluster_id, cluster_degree (cluster_link)
 * Output: link_id, partition (link_partition)
 */
class LinkPartitionMapper extends Mapper[Object, Text, IntWritable, StringArrayWritable] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, StringArrayWritable]#Context) = {
    var parts = value.toString().split("\\s+")
    context.write(new IntWritable(parts(0).toInt), new StringArrayWritable(Array(parts(1), parts(2))))
  }
}

class LinkPartitionReducer extends Reducer[IntWritable, StringArrayWritable, IntWritable, IntWritable] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[IntWritable, StringArrayWritable, IntWritable, IntWritable]#Context) = {
    var maxDegree = -1
    var maxCluster = -1
    values.foreach(value => {
      var parts = value.toStrings();
      var degree = parts(1).toInt
      var cluster = parts(0).toInt
      if (degree > maxDegree) {
        maxDegree = degree
        maxCluster = cluster
      }
    })
    var partition = Math.abs(maxCluster.toString.hashCode()) % Param.partition_count;
    context.write(key, new IntWritable(partition))
  }
}