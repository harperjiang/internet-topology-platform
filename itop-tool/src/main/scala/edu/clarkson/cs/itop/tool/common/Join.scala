package edu.clarkson.cs.itop.tool.common

import java.util.Arrays

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import edu.clarkson.cs.itop.tool.Utils
import edu.clarkson.cs.itop.tool.types.StringArrayWritable

class SingleKeyJoinMapper(left: String, right: String, leftJoinIndex: Int, rightJoinIndex: Int)
  extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {

  var leftTableName = left;
  var rightTableName = right;
  var leftKeyIndex = leftJoinIndex;
  var rightKeyIndex = rightJoinIndex;

  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context) = {
    var fileName = Utils.fileName(context.getInputSplit.asInstanceOf[FileSplit])
    var parts = value.toString().split("\\s+")
    fileName match {
      case s if s == leftTableName => {
        var values = Array("0");
        values ++= parts
        context.write(new StringArrayWritable(Array(parts(leftKeyIndex).toString(), "0")), new StringArrayWritable(values))
      }
      case s if s == rightTableName => {
        var values = Array("1");
        values ++= parts
        context.write(new StringArrayWritable(Array(parts(rightKeyIndex).toString(), "1")), new StringArrayWritable(values))
      }
      case _ => { throw new IllegalArgumentException(fileName); }
    }
  }
}

class JoinReducer(filter: (Text, Array[Writable], Array[Writable]) => Boolean,
  formatter: (Text, Array[Writable], Array[Writable]) => (Text, Text))
  extends Reducer[StringArrayWritable, StringArrayWritable, Text, Text] {

  override def reduce(key: StringArrayWritable, values: java.lang.Iterable[StringArrayWritable],
    context: Reducer[StringArrayWritable, StringArrayWritable, Text, Text]#Context) = {

    var buffer = scala.collection.mutable.ListBuffer[Array[Writable]]()
    var singlekey = new Text(key.get()(0).toString())
    values.foreach(value => {
      var index = value.get()(0).toString.toInt
      index match {
        case 0 => {
          // Left table
          var sub = Arrays.copyOfRange(value.get(), 1, value.get.length)
          buffer += sub
        }
        case 1 => {
          var rsub = Arrays.copyOfRange(value.get(), 1, value.get.length)
          buffer.foreach(sub => {
            if (filter == null || filter(singlekey, sub, rsub)) {
              if (formatter == null) {
                context.write(singlekey, new Text((sub ++ rsub).map { _.toString }.mkString("\t")))
              } else {
                var pair = formatter(singlekey, sub, rsub);
                context.write(pair._1, pair._1)
              }
            }
          })
        }
        case _ => { throw new IllegalArgumentException("Unrecognized number:" + index); }
      }
    })
  }
}