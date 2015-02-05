package edu.clarkson.cs.itop.tool.routing

import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import edu.clarkson.cs.itop.tool.Utils
import scala.collection.JavaConversions._

/**
 * Input:
 *          link_id, node_id
 *          link_id, cluster_id
 * Output:
 *          node_id, link_id, cluster_id
 */
class JoinClusterAndNodeMapper extends Mapper[Object, Text, IntWritable, ArrayWritable] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, ArrayWritable]#Context) = {
    var data = value.toString().split("\\s");
    Utils.fileName(context.getInputSplit.asInstanceOf[FileSplit]) match {
      case "node_link" => {
        context.write(new IntWritable(data(0).toInt), new ArrayWritable(Array("node_link", data(1))));
      }
      case "init_cluster_result" => {
        context.write(new IntWritable(data(0).toInt), new ArrayWritable(Array("link_cluster", data(1))));
      }
    }
  }
}

class JoinClusterAndNodeReducer extends Reducer[IntWritable, ArrayWritable, IntWritable, Text] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[ArrayWritable],
    context: Reducer[IntWritable, ArrayWritable, IntWritable, Text]#Context) = {
    var nodes = new scala.collection.mutable.HashSet[Int]();
    var cluster = "";
    values.foreach(value => {
      value.get()(0).toString() match {
        case "node_link" => {
          nodes += value.get()(1).toString().toInt;
        }
        case "link_cluster" => {
          cluster = value.get()(1).toString();
        }
      }
    });
    nodes.foreach(node => {
      context.write(new IntWritable(node), new Text(Array(key.toString(), cluster).mkString(" ")));
    });
  }
}

/**
 * Input:  node_id, link_id, cluster_id
 * Output: cluster_id_A, cluster_id_B, node_id
 */
class AdjacentLinkMapper extends Mapper[Object, Text, IntWritable, ArrayWritable] {

  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, IntWritable, ArrayWritable]#Context) = {
    var data = value.toString().split("\\s");
    context.write(new IntWritable(data(0).toInt), new ArrayWritable(Array(data(1), data(2))));
  }

}

class AdjacentLinkReducer extends Reducer[IntWritable, ArrayWritable, ArrayWritable, IntWritable] {

  override def reduce(key: IntWritable, values: java.lang.Iterable[ArrayWritable],
    context: Reducer[IntWritable, ArrayWritable, ArrayWritable, IntWritable]#Context) = {

    var ctlMap = new java.util.HashSet[String];
    values.foreach(value => {
      var cluster = value.get()(2).toString;
      ctlMap.add(cluster);
    });
    var list = ctlMap.toList;
    for (i <- 0 to list.size - 1) {
      for (j <- i + 1 to list.size - 1) {
        context.write(new ArrayWritable(Array(list(i), list(j))), key);
      }
    }
  }
}