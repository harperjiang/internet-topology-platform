package edu.clarkson.cs.itop.core.model

import scala.io.Source
import org.springframework.beans.factory.InitializingBean
import edu.clarkson.cs.itop.core.parser.Parser
import scala.beans.BeanProperty

/**
 * Partition is the manager of everything in a machine
 */
class Partition extends InitializingBean {

  @BeanProperty var id = 0;
  var nodeFile = "";
  var linkFile = "";
  /**
   * Mapping from IP address to node
   */
  val nodeIpMap = scala.collection.mutable.Map[String, Node]();
  val nodeMap = scala.collection.mutable.Map[Int, Node]();

  /**
   * Routing table
   */
  var routing: Routing = null;

  def afterPropertiesSet = {
    var parser = new Parser();

    // Load nodes, links from file
    var nfName = "%s_%d".format(nodeFile, this.id);
    var lfName = "%s_%d".format(linkFile, this.id);

    Source.fromFile(nfName).getLines.filter(!_.startsWith("#"))
      .map[Node](line => { parser.parse[Node](line) })
      .foreach(node => {
        nodeMap += (node.id -> node);
        node.ips.foreach(ip => { nodeIpMap += (ip -> node) })
      });

    var nodeNotFound = { node_id: Int => { throw new IllegalArgumentException("Node not found: %d".format(node_id)); } };

    Source.fromFile(lfName).getLines.filter(!_.startsWith("#"))
      .map[Link](line => { parser.parse[Link](line) })
      .foreach(link => {
        // Attach Link with nodes
        link.attachNodes(nodeMap);
        // Attach node with links
        link.namedNodes.foreach(entry => {
          entry._2.appendLink(link, entry._1);
        });
        link.anonymousNodes.foreach({ _.appendLink(link) });
      });
  }

  def queryPartition(node: Node): Iterable[Int] = {
    routing.route(node.id);
  }

}