package edu.clarkson.cs.itop.tool.partition.degree

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import edu.clarkson.cs.itop.tool.Utils
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import scala.collection.JavaConversions._

/**
 *  This Map/Reduce expand node to links that is one more step away from it. This task should be executed with key grouper
 *
 *  Example:
 *
 *  Input:   node_id(root_node) node_id(bound_node)   (Include node that is on n-1 step of the node)
 *  Input:   node_id(root_node) node_id(bound_node)   (Include links that is on n step of the node)
 *  Input:   node_id(from_node) node_id(to_node)   (adjacent node)
 *
 *  Output:  node_id(root_node) node_id(bound_node)   (Include nodes that is on n+1 step of the node)
 */

/**
 *  Input: node_id node_id (links at step n)
 *  Input: node_id node_id (adjacent nodes)
 *  Output: node_id link_id  (nodes at step n+1 and step n-1)
 */
class NodeExpandMapper extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context): Unit = {
    var fileName = Utils.fileName(context.getInputSplit.asInstanceOf[FileSplit])
    var values = value.toString().split("\\s+")
    fileName match {
      case "adj_node" => { // Original version    
        context.write(new StringArrayWritable(Array(values(0), "adj_node")), new StringArrayWritable(Array("adj_node", values(1))));
        context.write(new StringArrayWritable(Array(values(1), "adj_node")), new StringArrayWritable(Array("adj_node", values(0))));
      }
      case s if s.startsWith("node_expand_") => { // Expanded version
        context.write(new StringArrayWritable(Array(values(1), "node_expand")), new StringArrayWritable(Array("node_expand", values(0))));
      }
    }
  }
}

class NodeExpandReducer extends Reducer[StringArrayWritable, StringArrayWritable, IntWritable, IntWritable] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, IntWritable, IntWritable]#Context): Unit = {

    var adjList = scala.collection.mutable.ListBuffer[Int]();

    values.foreach(value => {
      var group = value.get()(0).toString();
      group match {
        case "adj_node" => {
          adjList += value.get()(1).toString().toInt;
        }
        case "node_expand" => {
          var rootNode = value.get()(1).toString().toInt;
          adjList.foreach { bound =>
            {
              if (rootNode != bound)
                context.write(new IntWritable(rootNode), new IntWritable(bound))
            }
          };
        }
      }
    });
  }
}
