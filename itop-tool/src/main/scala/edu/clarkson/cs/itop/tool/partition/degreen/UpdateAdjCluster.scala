package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import edu.clarkson.cs.itop.tool.Utils
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import scala.collection.JavaConversions._

/**
 * Input: small_cluster, big_cluster (adj_cluster)
 * Input: from_cluster, to_cluster (merge_decision)
 * Output: small_cluster, big_cluster (all appearance of from_cluster should be replaced by to_cluster)
 */
class UpdateAdjClusterMapper extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context) = {

    var parts = value.toString().split("\\s+")
    var fileName = Utils.fileName(context.getInputSplit.asInstanceOf[FileSplit])
    fileName match {
      case "merge_decision" => {
        context.write(new StringArrayWritable(Array(parts(0), "0")), new StringArrayWritable(Array("0", parts(1))))
      }
      case "adj_cluster" => {
        context.write(new StringArrayWritable(Array(parts(0), "1")), new StringArrayWritable(Array("1", parts(1))))
        context.write(new StringArrayWritable(Array(parts(1), "1")), new StringArrayWritable(Array("1", parts(0))))
      }
    }
  }
}

class UpdateAdjClusterReducer extends Reducer[StringArrayWritable, StringArrayWritable, IntWritable, IntWritable] {
  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, IntWritable, IntWritable]#Context): Unit = {
    var keyparts = key.toStrings();
    var mergeReplace: Int = -1;
    values.foreach(value => {
      var valueparts = value.toStrings();

      valueparts(0) match {
        case "0" => {
          mergeReplace = valueparts(1).toInt;
        }
        case "1" => {
          if (mergeReplace == -1) {
            return ;
          }
          var counterpart = valueparts(1).toInt
          if (mergeReplace != counterpart) {
            context.write(new IntWritable(Math.min(mergeReplace, counterpart)),
              new IntWritable(Math.max(mergeReplace, counterpart)))
          }
        }
      }
    })
  }
}