package edu.clarkson.cs.itop

import org.springframework.context.support.ClassPathXmlApplicationContext
import edu.clarkson.cs.itop.dist.MasterNode

class MasterUnit {

  var node: MasterNode = null;
}

object RunMaster extends App {
  var appContext = new ClassPathXmlApplicationContext("app-context-master.xml");
}