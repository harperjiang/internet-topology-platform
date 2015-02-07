package edu.clarkson.cs.itop.tool.partition.degree

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import edu.clarkson.cs.itop.tool.Utils
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

/**
 *  This Map/Reduce expand node to links that is one more step away from it.
 *
 *  Example:
 *
 *  Input:   node_id node_id   (Include node that is on n-1 step of the node)
 *  Input:   node_id node_id   (Include links that is on n step of the node)
 *  Input:   node_id node_id   (adjacent node)
 *
 *  Output:  node_id node_id   (Include nodes that is on n+1 step of the node)
 */

/**
 *  Input: node_id node_id (links at step n)
 *  Input: node_id node_id (adjacent nodes)
 *  Output: node_id link_id  (nodes at step n+1 and step n-1)
 */
class NodeExpandMapper extends Mapper[Object, Text, StringArrayWritable, IntWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, IntWritable]#Context): Unit = {
    var fileName = Utils.fileName(context.getInputSplit.asInstanceOf[FileSplit])
    var values = value.toString().split("\\s")
    fileName match {
      case "adj_node" => { // Original version    
          context.write(new StringArrayWritable(Array(values(0),"adj_node")), new IntWritable(values(1).toInt));
          context.write(new StringArrayWritable(Array(values(1),"adj_node")), new IntWritable(values(0).toInt));
      }
      case s if s.startsWith("node_expand_") => { // Expanded version
          context.write(new StringArrayWritable(Array(values(1),"node_expand")), new IntWritable(values(0).toInt));
      }
    }
  }
}

class NodeExpandReducer extends Reducer[StringArrayWritable, IntWritable, IntWritable, IntWritable] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[StringArrayWritable, IntWritable, IntWritable, IntWritable]#Context): Unit = {

  }
}

/**
 *  Input: node_id link_id (links at step n+1 and step n-1)
 *  Input: node_id link_id (links at step n-1)
 *  Output: node_id link_id  (links at step n+1)
 */
class RetroFilterMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {

  }
}

class RetroFilterReducer extends Reducer[IntWritable, IntWritable, IntWritable, IntWritable] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, IntWritable]#Context): Unit = {

  }
}