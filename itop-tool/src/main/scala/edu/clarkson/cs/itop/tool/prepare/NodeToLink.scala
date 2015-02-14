package edu.clarkson.cs.itop.tool.prepare

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import edu.clarkson.cs.itop.core.parser.Parser
import edu.clarkson.cs.itop.core.model.Link

/**
 * This mapper convert link to : link_id node_id
 */
class NodeToLinkMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  var parser = new Parser();

  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
    var line = value.toString();
    if (line.startsWith("#")) {
      return ;
    }

    var link = parser.parse[Link](line);
    var key = new IntWritable(link.id);
    link.anonymousNodeIds.foreach(node => { context.write(key, new IntWritable(node)) });
    link.namedNodeIds.foreach(node => { context.write(key, new IntWritable(node._2)) });
  }

}