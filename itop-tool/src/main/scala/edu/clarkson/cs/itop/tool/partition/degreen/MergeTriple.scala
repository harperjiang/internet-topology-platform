package edu.clarkson.cs.itop.tool.partition.degreen

import edu.clarkson.cs.itop.tool.common.SingleKeyJoinMapper
import edu.clarkson.cs.itop.tool.common.LeftOuterJoinReducer
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text

/**
 * Input: triple_old (node_id core_id degree length)
 * Input: triple_new (node_id core_id degree length)
 * Output: if triple_new exists for a node, use it, otherwise keep triple_old
 */
class MergeTripleMapper extends SingleKeyJoinMapper("triple_old", "triple_new", 0, 0);

class MergeTripleReducer extends LeftOuterJoinReducer(null,
  (key: Text, left: Array[Writable], right: Array[Writable]) => {
    if (right == null)
      (key, new Text(left.map(_.toString).mkString("\t")))
    else {
      (key, new Text(right.map(_.toString).mkString("\t")))
    }
  });