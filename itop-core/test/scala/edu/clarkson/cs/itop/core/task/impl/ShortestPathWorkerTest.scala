package edu.clarkson.cs.itop.core.task.impl

import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import edu.clarkson.cs.itop.core.model.Partition
import javax.annotation.Resource
import org.junit.Before
import edu.clarkson.cs.itop.core.task.Task
import edu.clarkson.cs.itop.core.task.TaskContext
import edu.clarkson.cs.itop.core.store.MemoryStore

@RunWith(classOf[SpringJUnit4ClassRunner])
@ContextConfiguration(locations = Array("classpath:app-context-worker.xml"))
class ShortestPathWorkerTest {

  @Resource
  var partition: Partition = null

  var kvstore = new MemoryStore();

  var task: Task = null;

  var subtask: Task = null;

  @Before
  def prepareTask: Unit = {
    var tc = new TaskContext((1, "Test-Task-Id"), null, partition, kvstore);
    task = new Task();
    task.id = (1, "Test-Task-Id");
    task.parent = null;
    task.root = task.id;
    task.context = tc;

    subtask = new Task();
    subtask.id = (2, "Test-Subtask-Id");
    subtask.parent = (1, "Test-SubTaskRoot-Id");
    subtask.root = task.id;
    subtask.context = tc;
  }

  @Test
  def testStart: Unit = {
    var worker = new ShortestPathWorker();
    
    worker.start(task);
    worker.start(subtask);
  }

  @Test
  def testSpawnTo: Unit = {

  }

  @Test
  def testCollect: Unit = {

  }

  @Test
  def testDone: Unit = {

  }

  @Test
  def testWorkOn: Unit = {

  }
}