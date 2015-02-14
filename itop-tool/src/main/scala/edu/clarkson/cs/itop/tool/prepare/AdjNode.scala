package edu.clarkson.cs.itop.tool.prepare

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.parser.Parser
import edu.clarkson.cs.scala.common.HeapSorter
import edu.clarkson.cs.itop.tool.types.IntArrayWritable

class AdjNodeMapper extends Mapper[Object, Text, IntArrayWritable, IntWritable] {
  var parser = new Parser();
  var one = new IntWritable(1);
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntArrayWritable, IntWritable]#Context): Unit = {
    var line = value.toString();
    if (!line.startsWith("#")) {
      var nodelink = parser.parse[Link](line);
      var nodes = List[Int]();
      nodes ++= nodelink.anonymousNodeIds;
      nodes ++= nodelink.namedNodeIds.map(node_id => { node_id._2 });

      var sorted = new HeapSorter().sort[Int](nodes, (a: Int, b: Int) => { a - b })

      for (i <- 0 to sorted.length - 1) {
        for (j <- i + 1 to sorted.length - 1) {
          var first = sorted(i);
          var second = sorted(j);
          if (first != second)
            context.write(new IntArrayWritable(Array(first, second).map(i => i.toString())), new IntWritable(second));
        }
      }
    }
  }
}

class AdjNodeReducer extends Reducer[IntArrayWritable, IntWritable, IntWritable, IntWritable] {
  override def reduce(key: IntArrayWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntArrayWritable, IntWritable, IntWritable, IntWritable]#Context): Unit = {
    context.write(new IntWritable(key.get()(0).toString().toInt), new IntWritable(key.get()(1).toString.toInt));
  }
}