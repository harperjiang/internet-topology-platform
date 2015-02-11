package edu.clarkson.cs.itop.tool.routing

import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.iterableAsScalaIterable
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import edu.clarkson.cs.itop.tool.Utils
import edu.clarkson.cs.itop.tool.common.JoinReducer
import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import org.apache.hadoop.io.Writable
import scala.io.Source
import java.util.Arrays
import java.net.URI
import java.net.URL

/**
 * Input:
 *          link_id, node_id (node_link)
 *          link_id, partition_id (link_partition)
 * Output:
 *          node_id, link_id, partition_id
 */
class NodeMachineMapper extends SingleKeyJoinMapper("node_link", "link_partition", 0, 0) {

}

class NodeMachineReducer extends JoinReducer(null, (key: Text, left: Array[Writable], right: Array[Writable]) => {
  (new Text(left(1).toString()), new Text(PartitionToMachine.machine(right(1).toString().toInt)))
}) {}

object PartitionToMachine {

  var map: scala.collection.mutable.Map[Int, String] = new scala.collection.mutable.HashMap[Int, String];

  {
    try {
      var source = ClassLoader.getSystemClassLoader().getResource("edu/clarkson/cs/itop/tool/routing/machine_partition");
      Source.fromFile(source.toURI())
        .getLines().foreach(line => {
          var parts = line.split("\\s+")
          var data = Arrays.copyOfRange(parts, 1, parts.length).mkString("\t")
          map += { parts(0).toInt -> data }
        })
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  def machine(partition: Int): String = {
    map.get(partition).getOrElse({ throw new IllegalArgumentException() });
  }
}