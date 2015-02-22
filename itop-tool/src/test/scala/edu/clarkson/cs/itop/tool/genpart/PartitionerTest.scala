package edu.clarkson.cs.itop.tool.genpart

import org.junit.Test
import java.io.File

class PartitionerTest {

  @Test
  def testNodePartition:Unit = {
    Partitioner.partitionNode(new File("testdata/genpart/partition_node/input"), new File("testdata/genpart/partition_node"))
  }
  
  @Test
  def testLinkPartition:Unit = {
    Partitioner.partitionLink(new File("testdata/genpart/partition_link/input"), new File("testdata/genpart/partition_link"))
  }
}