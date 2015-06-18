package edu.clarkson.cs.itop.app.traceroute

import edu.clarkson.cs.itop.core.task.TaskWorker
import edu.clarkson.cs.itop.core.task.impl.DFSTaskWorker
import edu.clarkson.cs.itop.core.task.Task
import edu.clarkson.cs.itop.core.task.impl.Path
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.task.TaskParam

class FixedLengthPathWorker extends DFSTaskWorker {

  var destination: String = null;

  override def start(t: Task) = {
    super.start(t);
    destination = TaskParam.getString(t, "destination");
  }

  override def test(node: Node): Boolean = {
    return node.ips.contains(destination) && currentPath.length == expectedDepth;
  }

  override def savePath(path: Path): Boolean = {
    if (existedPath == null) {
      existedPath = path.clonePath();
    }
    return false;
  }

  override def checkPath(path: Path): Boolean = {
    return path.length < expectedDepth;
  }

  override def process(node: Node): Unit = {

  }
}