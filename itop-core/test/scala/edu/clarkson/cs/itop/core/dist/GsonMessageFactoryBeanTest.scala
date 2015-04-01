package edu.clarkson.cs.itop.core.dist

import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import org.junit.Test
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import com.google.gson.Gson
import javax.annotation.Resource
import org.junit.Assert._
import edu.clarkson.cs.itop.core.dist.message.Heartbeat
import edu.clarkson.cs.itop.core.dist.message.SubtaskResult
import edu.clarkson.cs.itop.core.dist.message.SubtaskExecute
import edu.clarkson.cs.scala.common.message.KVStore

@RunWith(classOf[SpringJUnit4ClassRunner])
@ContextConfiguration(locations = Array("classpath:app-context-worker.xml"))
class GsonMessageFactoryBeanTest {

  @Resource(name = "gsonTranslator")
  var gson: Gson = null;

  @Test
  def testHeartbeatSD = {
    var hb = new Heartbeat(2, 3);
    assertEquals("{\"machine_id\":3,\"group_id\":2}", gson.toJson(hb));

    var jsonstr = gson.toJson(hb);
    var hb2 = gson.fromJson(jsonstr, classOf[Heartbeat]);
    assertEquals(hb.machineId, hb2.machineId);
    assertEquals(hb.groupId, hb2.groupId);
  }

  @Test
  def testSubtaskExecuteSD = {
    var se = new SubtaskExecute();
    se.parentMachine = 4;
    se.parentTaskId = "32-32-32";
    se.targetPartition = 4;
    se.targetNodeId = 434;
    se.workerClass = "DemoClass";

    assertEquals("{\"parent_machine\":4,\"worker_class\":\"DemoClass\",\"parent_task_id\":\"32-32-32\",\"root_machine\":0,\"root_task_id\":\"\",\"target_node_id\":434,\"target_partition\":4}", gson.toJson(se));

    var se2 = gson.fromJson(gson.toJson(se), classOf[SubtaskExecute]);
    assertEquals(se.parentId, se2.parentId);
    assertEquals(se.targetPartition, se2.targetPartition);
    assertEquals(se.targetNodeId, se2.targetNodeId);
    assertEquals(se.workerClass, se2.workerClass);
  }

  @Test
  def testSubtaskResultSD = {
    var sr = new SubtaskResult();
    sr.parentMachine = 4;
    sr.parentTaskId = "2423234wfa";
    sr.sourcePartitionId = 3;

    var sr2 = gson.fromJson(gson.toJson(sr), classOf[SubtaskResult]);

    assertEquals(sr.parentId, sr2.parentId);
    assertEquals(sr.sourcePartitionId, sr2.sourcePartitionId);
  }
}