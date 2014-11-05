package edu.clarkson.cs.itop

import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.junit.Test
import javax.annotation.Resource

@RunWith(classOf[SpringJUnit4ClassRunner])
@ContextConfiguration(locations = Array("classpath:app-context-worker.xml"))
class WorkUnitTest {

  @Resource
  var wu: WorkerUnit = null;

  @Test
  def testSubmitTask = {

  }

  @Test
  def testReceivedSubtask = {

  }
}