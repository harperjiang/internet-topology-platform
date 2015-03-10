package edu.clarkson.cs.itop.core.task.impl

import scala.collection.JavaConversions._
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import com.google.gson.Gson
import edu.clarkson.cs.itop.core.model.Partition
import edu.clarkson.cs.itop.core.store.MemoryStore
import edu.clarkson.cs.itop.core.task.Task
import edu.clarkson.cs.itop.core.task.TaskContext
import javax.annotation.Resource
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

@RunWith(classOf[SpringJUnit4ClassRunner])
@ContextConfiguration(locations = Array("classpath:app-context-worker.xml"))
class ShortestPathWorkerTest {

  @Resource
  var partition: Partition = null

  var kvstore: MemoryStore = null;

  var task: Task = null;

  var subtask: Task = null;

  @Before
  def prepareTask: Unit = {
    kvstore = new MemoryStore();
    var tc = new TaskContext((1, "Test-Task-Id"), null, partition, kvstore);
    task = new Task();
    task.id = (1, "Test-Task-Id");
    task.parent = null;
    task.root = task.id;
    task.context = tc;
    task.startNodeId = 1;

    subtask = new Task();
    subtask.id = (2, "Test-Subtask-Id");
    subtask.parent = (1, "Test-SubTaskRoot-Id");
    subtask.root = task.id;
    subtask.context = tc;
    subtask.startNodeId = 1;

  }

  @Test
  def testStart: Unit = {
    var worker = new ShortestPathWorker();
    worker.expectedDepth = 10;
    var subworker = new ShortestPathWorker();

    kvstore.set("1-Test-Task-Id.1-1.depthRemain", "5");

    worker.start(task);
    subworker.start(subtask);

    assertEquals(1, worker.currentPath.length);
    var pn = worker.currentPath.pop;
    assertEquals(1, pn.node.id);
    assertEquals(null, pn.link);
    assertEquals(null, pn.nodeIndex);
    assertEquals(null, pn.linkIndex);

    assertEquals(1, subworker.currentPath.length);
    var spn = subworker.currentPath.pop;
    assertEquals(1, spn.node.id);
    assertEquals(null, spn.link);
    assertEquals(null, spn.nodeIndex);
    assertEquals(null, spn.linkIndex);
    assertEquals(5, subworker.expectedDepth);
  }

  @Test
  def testSpawnTo: Unit = {
    var worker = new ShortestPathWorker();
    worker.expectedDepth = 10;
    worker.start(task);

    worker.spawnTo(task, 2, 3);

    assertEquals("9", kvstore.get("1-Test-Task-Id.3-2.depthRemain"));

    var path = kvstore.getObject("1-Test-Task-Id.3-2.path", classOf[Path]);
    assertEquals(1, path.length);
    var node = path.pop;
    assertEquals(1, node.node.id);
    assertEquals(null, node.link);
    assertEquals(null, node.nodeIndex);
    assertEquals(null, node.linkIndex);
  }

  @Test
  def testCollect: Unit = {
//    kvstore.setObject(key, value)
  }

  @Test
  def testDone: Unit = {

  }

  @Test
  def testWorkOn: Unit = {
var worker = new ShortestPathWorker();
    worker.expectedDepth = 10;
    worker.destId = 5;
    worker.start(task);

    var result = worker.workon(task, partition.nodeMap.get(1).get);

    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.1"));
    assertTrue(result._1);
    assertEquals(7, result._2.get.id)

    assertEquals(2, worker.currentPath.length);
    assertEquals(1, worker.currentPath.nodes(0).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.1"));
    assertEquals(7, worker.currentPath.nodes(1).node.id);

    result = worker.workon(task, result._2.get);

    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.1"));
    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.7"));
    assertTrue(result._1);
    assertEquals(3, result._2.get.id);

    assertEquals(3, worker.currentPath.length);
    assertEquals(1, worker.currentPath.nodes(0).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.1"));
    assertEquals(7, worker.currentPath.nodes(1).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.2"));
    assertEquals(3, worker.currentPath.nodes(2).node.id);

    result = worker.workon(task, result._2.get);

    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.1"));
    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.7"));
    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.3"));
    assertTrue(result._1);
    assertEquals(4, result._2.get.id);

    assertEquals(4, worker.currentPath.length);
    assertEquals(1, worker.currentPath.nodes(0).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.1"));
    assertEquals(7, worker.currentPath.nodes(1).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.2"));
    assertEquals(3, worker.currentPath.nodes(2).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.3"));
    assertEquals(4, worker.currentPath.nodes(3).node.id);

    result = worker.workon(task, result._2.get);

    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.1"));
    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.7"));
    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.3"));
    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.4"));
    assertTrue(result._1);
    assertEquals(9, result._2.get.id);

    assertEquals(5, worker.currentPath.length);
    assertEquals(1, worker.currentPath.nodes(0).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.1"));
    assertEquals(7, worker.currentPath.nodes(1).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.2"));
    assertEquals(3, worker.currentPath.nodes(2).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.3"));
    assertEquals(4, worker.currentPath.nodes(3).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.4"));
    assertEquals(9, worker.currentPath.nodes(4).node.id);

    result = worker.workon(task, result._2.get);

    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.1"));
    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.7"));
    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.3"));
    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.4"));
    assertEquals("true", kvstore.get("1-Test-Task-Id.nodeVisited.9"));

    assertTrue(result._1);
    assertEquals(4, result._2.get.id);

    assertEquals(4, worker.currentPath.length);
    assertEquals(1, worker.currentPath.nodes(0).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.1"));
    assertEquals(7, worker.currentPath.nodes(1).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.2"));
    assertEquals(3, worker.currentPath.nodes(2).node.id);
    assertEquals("true", kvstore.get("1-Test-Task-Id.linkVisited.3"));
    assertEquals(4, worker.currentPath.nodes(3).node.id);
    assertEquals("false", kvstore.get("1-Test-Task-Id.linkVisited.4"));
  }
}