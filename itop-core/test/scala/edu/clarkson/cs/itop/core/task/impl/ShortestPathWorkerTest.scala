package edu.clarkson.cs.itop.core.task.impl

import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.model.Node
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

    assertEquals("9", kvstore.get("1-Test-Task-Id.2-3.depthRemain"));

    var path = kvstore.getObject("1-Test-Task-Id.2-3.path", classOf[Path]);
    assertEquals(1, path.length);
    var node = path.pop;
    assertEquals(1, node.node.id);
    assertEquals(null, node.link);
    assertEquals(null, node.nodeIndex);
    assertEquals(null, node.linkIndex);
  }

  @Test
  def testCollect: Unit = {
    var subpath = new Path();
    subpath.push(new PathNode(new Node(5), new Link(3), null, null));
    subpath.push(new PathNode(new Node(4), new Link(2), null, null));
    kvstore.setObject("1-Test-Task-Id.2-3.result", subpath);

    var path = new Path();
    path.push(new PathNode(new Node(1), null, null, null));
    path.push(new PathNode(new Node(3), new Link(9), null, null));
    kvstore.setObject("1-Test-Task-Id.2-3.path", path);

    var worker = new ShortestPathWorker();
    worker.collect(task, 2, 3);

    assertEquals(4, worker.existedPath.length);
  }

  @Test
  def testDone: Unit = {
	var worker = new ShortestPathWorker();
  }

  @Test
  def testWorkOn: Unit = {

  }
}