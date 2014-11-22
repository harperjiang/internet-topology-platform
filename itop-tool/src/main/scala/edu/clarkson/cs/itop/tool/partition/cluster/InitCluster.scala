package edu.clarkson.cs.itop.tool.partition.cluster

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.parser.Parser
import edu.clarkson.cs.itop.tool.Utils
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

/**
 * Input:
 *        node_link      :  link_id  node_id
 *        node_degree    :  node_id  degree
 * Output:
 *        node_id link_id node_degree
 */
class InitClusterMapper extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {
  val parser = new Parser();

  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context) = {
    var fileName = Utils.fileName(context.getInputSplit.asInstanceOf[FileSplit])
    fileName match {
      case "node_link" => {
        var values = value.toString().split("\\s+");
        context.write(new StringArrayWritable(Array(values(1), "node_link")),
          new StringArrayWritable(Array("node_link", values(0))));
      }
      case "node_degree" => {
        var values = value.toString().split("\\s+");
        context.write(new StringArrayWritable(Array(values(0), "node_degree")),
          new StringArrayWritable(Array("node_degree", values(1))));
      }
      case _ => { throw new IllegalArgumentException(fileName); }
    }
  }
}

class InitClusterReducer extends Reducer[StringArrayWritable, StringArrayWritable, IntWritable, Text] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, IntWritable, Text]#Context) = {
    var degree = 0;
    var links = new ArrayBuffer[Int]();
    var realkey = new IntWritable(key.get()(0).toString().toInt);
    values.foreach { value =>
      {
        value.get()(0).toString() match {
          case "node_degree" => { degree = value.get()(1).toString().toInt };
          case "node_link" => {
            context.write(realkey, new Text(Array(value.get()(1).toString(), degree).mkString("\t")))
          };
          case _ => { throw new IllegalArgumentException(value.get()(0).toString()) };
        }
      }
    };
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
 * Input:   link_id node_id degree
 * Output:  link_id node_id degree (only the one with largest degree)
 */
class InitClusterReducer2 extends Reducer[IntWritable, Text, IntWritable, Text] {
  override def reduce(key: IntWritable, values: java.lang.Iterable[Text],
    context: Reducer[IntWritable, Text, IntWritable, Text]#Context) = {
    var largest_degree = 0;
    var largest_node = -1;
    values.foreach(value => {
      var data = value.toString().split("\\s+");
      var current_degree = data(1).toInt;
      var current_node = data(0).toInt;
      if (current_degree > largest_degree) {
        largest_degree = current_degree;
        largest_node = current_node;
      }
    });
    context.write(key, new Text(Array(largest_node, largest_degree).map(_.toString).mkString("\t")))
  }
}

/**
 * This mapper convert link to : node_id link_id
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