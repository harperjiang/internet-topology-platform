package edu.clarkson.cs.itop.core.scheduler

import java.util.concurrent.ThreadFactory

class TaskThreadFactory extends ThreadFactory {

  def newThread(run: Runnable): Thread = {
    return null;
  }
}