package edu.clarkson.cs.itop.tool.genpart

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import edu.clarkson.cs.itop.tool.Utils
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import edu.clarkson.cs.itop.core.parser.Parser
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.core.model.Link
import scala.collection.JavaConversions._

/**
 * Input: link_id partition_id (link_partition)
 * Input: kapar-midar-iff.links
 * Output: partition_id link_info
 */
class LinkPartitionJoinMapper extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {

  var parser = new Parser();

  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context) = {
    var fileName = Utils.fileName(context.getInputSplit.asInstanceOf[FileSplit])

    fileName match {
      case "link_partition" => {
        var parts = value.toString().split("\\s+")
        context.write(new StringArrayWritable(Array(parts(0), "0")), new StringArrayWritable(Array("0", parts(1))))
      }
      case "kapar-midar-iff.links" => {
        if (!value.toString.startsWith("#")) {
          var link = parser.parse[Link](value.toString())
          context.write(new StringArrayWritable(Array(link.id.toString, "1")), new StringArrayWritable(Array("1", value.toString)))
        }
      }
    }
  }
}

class LinkPartitionJoinReducer extends Reducer[StringArrayWritable, StringArrayWritable, Text, Text] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, Text, Text]#Context) = {

    var buffer: String = null;
    values.foreach(value => {
      var parts = value.toStrings()
      parts(0) match {
        case "0" => { buffer = parts(1) }
        case "1" => { context.write(new Text(buffer), new Text(parts(1))) }
      }
    })
  }
}