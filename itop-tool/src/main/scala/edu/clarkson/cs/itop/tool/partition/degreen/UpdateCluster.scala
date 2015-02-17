package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.mapreduce.Mapper
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import edu.clarkson.cs.itop.tool.Utils
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._

/**
 * Input: cluster_id merge_time max_degree(cluster)
 * Input: cluster_from cluster_to(merge_decision)
 * Output: if cluster_id is a from, remove it
 * 		   if cluster_id is a to, increase merge_time by 1
 *         cannot be both cause all such cases will be removed in the refine process
 */

class UpdateClusterMapper extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {

  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context) = {
    var fileName = Utils.fileName(context.getInputSplit.asInstanceOf[FileSplit])
    var parts = value.toString().split("\\s+")
    fileName match {
      case "cluster" => {
        var values = Array("0");
        values ++= parts
        context.write(new StringArrayWritable(Array(parts(0), "0")), new StringArrayWritable(values))
      }
      case "merge_decision" => {
        var valuesFrom = Array("1", "0");
        var valuesTo = Array("1", "1");
        valuesFrom ++= parts
        valuesTo ++= parts
        context.write(new StringArrayWritable(Array(parts(0), "1")), new StringArrayWritable(valuesFrom))
        context.write(new StringArrayWritable(Array(parts(0), "1")), new StringArrayWritable(valuesTo))
      }
      case _ => { throw new IllegalArgumentException(fileName); }
    }
  }
}

class UpdateClusterReducer extends Reducer[StringArrayWritable, StringArrayWritable, Text, Text] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, Text, Text]#Context): Unit = {
    var cluster: StringArrayWritable = null;
    values.foreach(value => {
      var parts = value.toStrings();
      parts(0) match {
        case "0" => {
          cluster = value;
        }
        case "1" => {
          parts(1) match {
            case "0" => { // This is a from node, just ignore it
              return ;
            }
            case "1" => { // This is a to node, increase merge_times by one
              // Increase once and return makes sure that merge_times is increased by at most 1
              var data = cluster.get();
              var oldMerge = data(2).toString().toInt
              context.write(new Text(key.toStrings()(0)),
                new Text(Array((oldMerge + 1).toString, data(3).toString()).mkString("\t")));
            }
          }
        }
      }
    });
  }
}