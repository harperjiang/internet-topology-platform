package edu.clarkson.cs.itop.tool.partcomp

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 *  Input:  cluster_from cluster_to cluster_from_size cluster_to_size
 *  Output: from_node to_node from_size
 *  
 *  This output need to be distincted to remove redundancy in the case of only two nodes.
 */
class FindHeadTailMapper extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context) = {
    var parts = value.toString().split("\\s+");
    context.write(new StringArrayWritable(Array(parts(0), "0", parts(1))),
      new StringArrayWritable(Array("0", parts(0), parts(1), parts(2), parts(3))));
    context.write(new StringArrayWritable(Array(parts(1), "1", parts(0))),
      new StringArrayWritable(Array("1", parts(0), parts(1), parts(2), parts(3))));
  }
}

class FindHeadTailReducer extends Reducer[StringArrayWritable, StringArrayWritable, IntWritable, Text] {

  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, IntWritable, Text]#Context): Unit = {

    var realkey = key.toStrings()(0);
    var hasZero = false;
    var hasOne = false;
    // Record only one record
    var zeroBuffer: Array[String] = null;
    var oneBuffer: Array[String] = null;

    values.foreach(value => {
      var parts = value.toStrings();
      var flag = parts(0);
      flag match {
        case "0" => {
          hasZero = true;
          if (zeroBuffer == null)
            zeroBuffer = parts;
        }
        case "1" => {
          hasOne = true;
          if (oneBuffer == null) {
            oneBuffer = parts;
          }

          if (hasZero) {
            return ;
          }
          // Output the 1 result
          context.write(new IntWritable(parts(0).toInt), new Text(Array(parts(1), parts(2)).mkString("\t")));
          return ;
        }
      }
    });
    // Output the 0 result
    context.write(new IntWritable(zeroBuffer(0).toInt), new Text(Array(zeroBuffer(1), zeroBuffer(2)).mkString("\t")));
  }
}
