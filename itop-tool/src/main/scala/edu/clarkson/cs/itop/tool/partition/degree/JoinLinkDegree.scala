package edu.clarkson.cs.itop.tool.partition.degree

import scala.collection.JavaConversions._
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
class JoinLinkDegreeMapper extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {
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

class JoinLinkDegreeReducer extends Reducer[StringArrayWritable, StringArrayWritable, IntWritable, Text] {
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
