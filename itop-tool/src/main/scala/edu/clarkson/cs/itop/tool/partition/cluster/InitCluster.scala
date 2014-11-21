package edu.clarkson.cs.itop.tool.partition.cluster

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import edu.clarkson.cs.itop.core.parser.Parser
import edu.clarkson.cs.itop.core.model.Link
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._

/**
 * Input:
 *        link           :  node_id  link_id
 *        node_degree    :  node_id  degree
 */
class InitClusterMapper extends Mapper[Object, Text, IntWritable, Text] {
  val parser = new Parser();
  var linkFile = "";
  var nodeDegreeFile = "";

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context) = {
    var fileName = context.getInputSplit.asInstanceOf[FileSplit].getPath.getName;
    fileName match {
      case f if f == linkFile => {
        var line = value.toString();
        if (!line.startsWith("#")) {
          var nodelink = parser.parse[Link](line);
          var content = new Text();
          content.set(Array("link", nodelink.id.toString()).mkString(" "));
          nodelink.anonymousNodeIds.foreach { node_id => { context.write(new IntWritable(node_id), content); } };
          nodelink.namedNodeIds.foreach(node_id => { context.write(new IntWritable(node_id._2), content) })
        }
      }
      case f if f == nodeDegreeFile => {
        var values = value.toString().split("\\s");
        context.write(new IntWritable(values(0).toInt), new Text(Array("node_degree", values(1)).mkString(" ")));
      }
      case _ => { throw new IllegalArgumentException(); }
    }
  }
}

/**
 * Output:
 *        node_id link_id node_degree
 */
class InitClusterReducer extends Reducer[IntWritable, Text, IntWritable, Text] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[Text],
    context: Reducer[IntWritable, Text, IntWritable, Text]#Context) = {
    var data = new Array[Int](2);
    values.foreach { value =>
      {
        var parts = value.toString().split("\\s");
        var key = parts(0);
        key match {
          case "node_degree" => { data(1) = parts(1).toInt };
          case "link" => { data(0) = parts(1).toInt };
        }
      }
    };
    context.write(key, new Text(data.map(i => i.toString()).mkString(" ")))
  }
}

/**
 * Input:   node_id line_id degree
 * Output:  line_id node_id degree
 */
class InitClusterMapper2 extends Mapper[Object, Text, IntWritable, Text] {
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context) = {
    var data = value.toString().split("\\s");
    context.write(new IntWritable(data(1).toInt), new Text(Array(data(0), data(2)).mkString(" ")));
  }
}

/**
 * Input:   line_id node_id degree
 * Output:  line_id node_id degree (only the one with largest degree)
 */
class InitClusterReducer2 extends Reducer[IntWritable, Text, IntWritable, Text] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[Text],
    context: Reducer[IntWritable, Text, IntWritable, Text]#Context) = {
    var largest_degree = 0;
    var largest_node = -1;
    values.foreach(value => {
      var data = value.toString().split("\\s");
      var current_degree = data(1).toInt;
      var current_node = data(0).toInt;
      if (current_degree > largest_degree) {
        largest_degree = current_degree;
        largest_node = current_node;
      }
    });
    context.write(key, new Text(Array(largest_node, largest_degree).map(_.toString).mkString(" ")))
  }
}