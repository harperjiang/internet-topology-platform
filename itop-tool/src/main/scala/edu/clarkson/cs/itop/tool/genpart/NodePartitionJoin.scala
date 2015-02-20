package edu.clarkson.cs.itop.tool.genpart

import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.core.model.Link
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.io.Text
import edu.clarkson.cs.itop.tool.Utils
import edu.clarkson.cs.itop.core.parser.Parser
import scala.collection.JavaConversions._
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import edu.clarkson.cs.itop.tool.common.JoinReducer
import org.apache.hadoop.io.Writable

/**
 * Input: link_id, partition_id (link_partition)
 * Input: link_id, node_id (node_link)
 * Output: node_id, partition_id
 */
class NodePartitionJoinMapper extends SingleKeyJoinMapper("link_partition", "node_link", 0, 0)

class NodePartitionJoinReducer extends JoinReducer(
  null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => { (new Text(right(1).toString), new Text(left(1).toString)) })

/**
 * Input: kapar-midar-iff.nodes
 * Input: node_id, partition_id
 * Output: partition_id, node_info
 */
class NodePartitionInfoJoinMapper extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {
  var parser = new Parser();

  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context) = {
    var fileName = Utils.fileName(context.getInputSplit.asInstanceOf[FileSplit])

    fileName match {
      case "node_partition" => {
        var parts = value.toString().split("\\s+")
        context.write(new StringArrayWritable(Array(parts(0), "1")), new StringArrayWritable(Array("1", parts(1))))
      }
      case "kapar-midar-iff.nodes" => {
        if (!value.toString.startsWith("#")) {
          var node = parser.parse[Node](value.toString())
          context.write(new StringArrayWritable(Array(node.id.toString, "0")), new StringArrayWritable(Array("0", value.toString)))
        }
      }
    }
  }
}

class NodePartitionInfoJoinReducer extends Reducer[StringArrayWritable, StringArrayWritable, Text, Text] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, Text, Text]#Context) = {

    var buffer: String = null;
    values.foreach(value => {
      var parts = value.toStrings()
      parts(0) match {
        case "0" => { buffer = parts(1) }
        case "1" => { context.write(new Text(parts(1)), new Text(buffer)) }
      }
    })
  }
}