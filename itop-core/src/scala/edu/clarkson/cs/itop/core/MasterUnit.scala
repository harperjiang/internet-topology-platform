package edu.clarkson.cs.itop.core

import org.slf4j.LoggerFactory
import org.springframework.context.support.ClassPathXmlApplicationContext

import edu.clarkson.cs.itop.core.dist.MasterNode

class MasterUnit {

  var node: MasterNode = null;
  
}

object RunMaster extends App {
  var logger = LoggerFactory.getLogger(RunMaster.getClass)
  logger.info("Master Unit Started")
  var appContext = new ClassPathXmlApplicationContext("app-context-master.xml");
}