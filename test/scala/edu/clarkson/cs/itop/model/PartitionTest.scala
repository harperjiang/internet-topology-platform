package edu.clarkson.cs.itop.model;

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.Assert._
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import javax.annotation.Resource

@RunWith(classOf[SpringJUnit4ClassRunner])
@ContextConfiguration(locations = Array("classpath:app-context-worker.xml"))
class PartitionTest {

  @Resource(name = "partition")
  var partition: Partition = null;

  @Test
  def testLoadPartition = {
    assertEquals(10, partition.nodeMap.size);
    assertEquals(1, partition.nodeIpMap.get("23.13.15.29").get.id);
    assertEquals(10, partition.nodeIpMap.get("23.13.15.137").get.id);

    var n9 = partition.nodeMap.get(9).get;
    assertEquals(2, n9.anonymousLinks.size);

    var linkset = Set(n9.anonymousLinks(0).id, n9.anonymousLinks(1).id);

    assertTrue(linkset.contains(2));
    assertTrue(linkset.contains(4));
  }

}
